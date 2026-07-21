use anyhow::{Result, bail};

use crate::provider::types::{
    AssistantContent, AssistantMessage, ProviderContextFragment, ProviderEvent,
    PublicAssistantContent, PublicAssistantMessage, PublicMessage, StopReason, ToolResultMessage,
};
use crate::store::{DurableEvent, EventWrite, Projection};

use super::{AgentEvent, PublicStreamEvent};

/// Stateful projection of one normalized provider attempt into the public agent
/// event vocabulary. ProviderEventStream owns wire normalization; this seam
/// owns only Start/terminal ordering and the canonical public projection.
#[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
pub(crate) struct ProviderEventProjector {
    message_id: String,
    state: ProjectionState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectionState {
    AwaitingStart,
    Streaming,
    Terminal,
}

#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
pub(crate) enum ProjectedProviderEvent {
    Started,
    Update(AgentEvent),
    RejectedToolCall {
        event: AgentEvent,
        synthetic_result: ToolResultMessage,
    },
    Terminal(ProviderTerminal),
}

/// A terminal MessageEnd plus the opaque provider continuation material that
/// must eventually be persisted atomically with it. T17 owns that persistence
/// projection; retaining it here prevents this T15 seam from silently dropping
/// provider context or claiming it is durable.
#[derive(Clone, Debug, PartialEq)]
#[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
pub(crate) struct ProviderTerminal {
    event: AgentEvent,
    message: PublicMessage,
    provider_context: Vec<ProviderContextFragment>,
    kind: ProviderTerminalKind,
}

impl ProviderTerminal {
    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn event(&self) -> &AgentEvent {
        &self.event
    }

    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn message(&self) -> &PublicMessage {
        &self.message
    }

    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn provider_context(&self) -> &[ProviderContextFragment] {
        &self.provider_context
    }

    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn kind(&self) -> ProviderTerminalKind {
        self.kind
    }

    /// Builds the T12-representable half of terminal durability. Opaque
    /// provider context cannot be silently omitted: until T17 supplies its
    /// encrypted projection, only terminals without such context are writable.
    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn into_t12_write(
        self,
        run_id: impl Into<String>,
        turn_id: impl Into<String>,
        append_to_l0: bool,
    ) -> Result<EventWrite> {
        if !self.provider_context.is_empty() {
            bail!("provider terminal context requires the T17 persistence projection");
        }
        let message_id = match &self.event {
            AgentEvent::MessageEnd { message_id, .. } => message_id.clone(),
            _ => unreachable!("ProviderTerminal always contains MessageEnd"),
        };
        let run_id = run_id.into();
        let turn_id = turn_id.into();
        Ok(EventWrite {
            event: Some(DurableEvent::message_in_turn(
                "message_end",
                &message_id,
                &self.message,
                Some(run_id),
                Some(turn_id),
            )?),
            projections: vec![Projection::MessageEnd {
                message_id,
                role: "assistant",
                message: self.message,
                append_to_l0,
            }],
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
pub(crate) enum ProviderTerminalKind {
    Done,
    Error,
}

impl ProviderEventProjector {
    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn new(message_id: impl Into<String>) -> Result<Self> {
        let message_id = message_id.into();
        if message_id.is_empty() {
            bail!("provider projection message_id must not be empty");
        }
        Ok(Self {
            message_id,
            state: ProjectionState::AwaitingStart,
        })
    }

    #[allow(dead_code, reason = "consumed by the later T15 Session run loop")]
    pub(crate) fn project(&mut self, event: ProviderEvent) -> Result<ProjectedProviderEvent> {
        match self.state {
            ProjectionState::AwaitingStart => {
                if !matches!(event, ProviderEvent::Start) {
                    bail!("provider event arrived before Start");
                }
                self.state = ProjectionState::Streaming;
                Ok(ProjectedProviderEvent::Started)
            }
            ProjectionState::Streaming => self.project_streaming(event),
            ProjectionState::Terminal => bail!("provider event arrived after terminal event"),
        }
    }

    fn project_streaming(&mut self, event: ProviderEvent) -> Result<ProjectedProviderEvent> {
        let projected = match event {
            ProviderEvent::Start => bail!("provider emitted Start more than once"),
            ProviderEvent::TextStart { content_index } => {
                self.update(PublicStreamEvent::TextStart { content_index })
            }
            ProviderEvent::TextDelta {
                content_index,
                delta,
            } => self.update(PublicStreamEvent::TextDelta {
                content_index,
                delta,
            }),
            ProviderEvent::TextEnd {
                content_index,
                content,
            } => self.update(PublicStreamEvent::TextEnd {
                content_index,
                content,
            }),
            ProviderEvent::ThinkingStart {
                content_index,
                signature_field: _,
            } => self.update(PublicStreamEvent::ThinkingStart { content_index }),
            ProviderEvent::ThinkingDelta {
                content_index,
                delta,
            } => self.update(PublicStreamEvent::ThinkingDelta {
                content_index,
                delta,
            }),
            ProviderEvent::ThinkingEnd {
                content_index,
                content,
            } => self.update(PublicStreamEvent::ThinkingEnd {
                content_index,
                content,
            }),
            ProviderEvent::ToolCallStart { content_index } => {
                self.update(PublicStreamEvent::ToolCallStart { content_index })
            }
            ProviderEvent::ToolCallDelta {
                content_index,
                delta,
            } => self.update(PublicStreamEvent::ToolCallDelta {
                content_index,
                delta,
            }),
            ProviderEvent::ToolCallPreview {
                content_index,
                preview,
            } => self.update(PublicStreamEvent::ToolCallPreview {
                content_index,
                preview,
            }),
            ProviderEvent::ToolCallEnd {
                content_index,
                tool_call,
            } => self.update(PublicStreamEvent::ToolCallEnd {
                content_index,
                tool_call,
            }),
            ProviderEvent::ToolCallRejected {
                content_index,
                rejected,
                synthetic_result,
            } => ProjectedProviderEvent::RejectedToolCall {
                event: self.update_event(PublicStreamEvent::ToolCallRejected {
                    content_index,
                    rejected,
                }),
                synthetic_result,
            },
            ProviderEvent::ReasoningSummaryStart { content_index } => {
                self.update(PublicStreamEvent::ReasoningSummaryStart { content_index })
            }
            ProviderEvent::ReasoningSummaryDelta {
                content_index,
                delta,
            } => self.update(PublicStreamEvent::ReasoningSummaryDelta {
                content_index,
                delta,
            }),
            ProviderEvent::ReasoningSummaryEnd {
                content_index,
                content,
            } => self.update(PublicStreamEvent::ReasoningSummaryEnd {
                content_index,
                content,
            }),
            ProviderEvent::Done { reason, output } => {
                self.terminal(ProviderTerminalKind::Done, reason, output)?
            }
            ProviderEvent::Error { reason, output } => {
                self.terminal(ProviderTerminalKind::Error, reason, output)?
            }
        };
        Ok(projected)
    }

    fn update(&self, event: PublicStreamEvent) -> ProjectedProviderEvent {
        ProjectedProviderEvent::Update(self.update_event(event))
    }

    fn update_event(&self, event: PublicStreamEvent) -> AgentEvent {
        AgentEvent::MessageUpdate {
            message_id: self.message_id.clone(),
            event,
        }
    }

    fn terminal(
        &mut self,
        kind: ProviderTerminalKind,
        reason: StopReason,
        output: crate::provider::types::ProviderOutput,
    ) -> Result<ProjectedProviderEvent> {
        if output.message.stop_reason != reason {
            bail!("provider terminal reason does not match terminal message");
        }
        match kind {
            ProviderTerminalKind::Done
                if matches!(reason, StopReason::Error | StopReason::Aborted) =>
            {
                bail!("Done cannot close an Error or Aborted provider message");
            }
            ProviderTerminalKind::Error
                if !matches!(reason, StopReason::Error | StopReason::Aborted) =>
            {
                bail!("Error must close an Error or Aborted provider message");
            }
            _ => {}
        }

        let message = PublicMessage::Assistant(public_assistant_message(&output.message));
        let event = AgentEvent::MessageEnd {
            message_id: self.message_id.clone(),
            message: Box::new(message.clone()),
        };
        self.state = ProjectionState::Terminal;
        Ok(ProjectedProviderEvent::Terminal(ProviderTerminal {
            event,
            message,
            provider_context: output.provider_context,
            kind,
        }))
    }
}

fn public_assistant_message(message: &AssistantMessage) -> PublicAssistantMessage {
    PublicAssistantMessage {
        content: message
            .content
            .iter()
            .map(|content| match content {
                AssistantContent::Text {
                    text,
                    wire_item_index,
                } => PublicAssistantContent::Text {
                    text: text.clone(),
                    wire_item_index: *wire_item_index,
                },
                AssistantContent::Thinking {
                    thinking,
                    signature_field,
                    wire_item_index,
                } => PublicAssistantContent::Thinking {
                    thinking: thinking.clone(),
                    signature_field: signature_field.clone(),
                    wire_item_index: *wire_item_index,
                },
                AssistantContent::ToolCall {
                    tool_call,
                    wire_item_index,
                } => PublicAssistantContent::ToolCall {
                    tool_call: tool_call.clone(),
                    wire_item_index: *wire_item_index,
                },
                AssistantContent::RejectedToolCall {
                    rejected,
                    wire_item_index,
                } => PublicAssistantContent::RejectedToolCall {
                    rejected: rejected.clone(),
                    wire_item_index: *wire_item_index,
                },
            })
            .collect(),
        model: message.model.clone(),
        provider: message.provider.clone(),
        origin: message.origin.clone(),
        usage: message.usage.clone(),
        stop_reason: message.stop_reason,
        error_message: message.error_message.clone(),
        provider_code: message.provider_code.clone(),
        interrupted: message.interrupted,
        timestamp: message.timestamp,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;
    use crate::provider::types::{
        ApiProtocol, ProviderContextPayload, ProviderOrigin, ProviderOutput, RejectedToolCall,
        ToolArgumentError, ToolCall, Usage,
    };

    fn output(reason: StopReason) -> ProviderOutput {
        ProviderOutput {
            message: AssistantMessage {
                content: vec![
                    AssistantContent::Thinking {
                        thinking: "plan".to_owned(),
                        signature_field: "reasoning_content".to_owned(),
                        wire_item_index: 0,
                    },
                    AssistantContent::Text {
                        text: "answer".to_owned(),
                        wire_item_index: 1,
                    },
                ],
                model: "model".to_owned(),
                provider: "provider".to_owned(),
                origin: ProviderOrigin {
                    provider_instance_id: "instance".to_owned(),
                    protocol: ApiProtocol::OpenAiChatCompletions,
                    model: "model".to_owned(),
                },
                usage: Usage::default(),
                stop_reason: reason,
                error_message: (reason == StopReason::Error).then(|| "failed".to_owned()),
                provider_code: None,
                interrupted: reason == StopReason::Aborted,
                timestamp: Utc::now(),
            },
            provider_context: Vec::new(),
        }
    }

    fn started() -> ProviderEventProjector {
        let mut projector = ProviderEventProjector::new("message-1").expect("projector");
        assert_eq!(
            projector.project(ProviderEvent::Start).expect("Start"),
            ProjectedProviderEvent::Started
        );
        projector
    }

    #[test]
    fn projects_text_thinking_tool_and_summary_with_opaque_indices() {
        let mut projector = started();
        let tool_call: ToolCall = serde_json::from_value(json!({
            "id": "call-1",
            "name": "read",
            "arguments": {"path": "README.md"}
        }))
        .expect("tool call");
        let preview = crate::provider::types::ToolArgsPreview::new(json!({"path": "READ"}));
        let cases = vec![
            (
                ProviderEvent::TextStart { content_index: 3 },
                PublicStreamEvent::TextStart { content_index: 3 },
            ),
            (
                ProviderEvent::TextDelta {
                    content_index: 3,
                    delta: "a".to_owned(),
                },
                PublicStreamEvent::TextDelta {
                    content_index: 3,
                    delta: "a".to_owned(),
                },
            ),
            (
                ProviderEvent::TextEnd {
                    content_index: 3,
                    content: "answer".to_owned(),
                },
                PublicStreamEvent::TextEnd {
                    content_index: 3,
                    content: "answer".to_owned(),
                },
            ),
            (
                ProviderEvent::ThinkingStart {
                    content_index: 7,
                    signature_field: "reasoning_content".to_owned(),
                },
                PublicStreamEvent::ThinkingStart { content_index: 7 },
            ),
            (
                ProviderEvent::ThinkingDelta {
                    content_index: 7,
                    delta: "b".to_owned(),
                },
                PublicStreamEvent::ThinkingDelta {
                    content_index: 7,
                    delta: "b".to_owned(),
                },
            ),
            (
                ProviderEvent::ThinkingEnd {
                    content_index: 7,
                    content: "plan".to_owned(),
                },
                PublicStreamEvent::ThinkingEnd {
                    content_index: 7,
                    content: "plan".to_owned(),
                },
            ),
            (
                ProviderEvent::ToolCallStart { content_index: 11 },
                PublicStreamEvent::ToolCallStart { content_index: 11 },
            ),
            (
                ProviderEvent::ToolCallDelta {
                    content_index: 11,
                    delta: "{\"path\":".to_owned(),
                },
                PublicStreamEvent::ToolCallDelta {
                    content_index: 11,
                    delta: "{\"path\":".to_owned(),
                },
            ),
            (
                ProviderEvent::ToolCallPreview {
                    content_index: 11,
                    preview: preview.clone(),
                },
                PublicStreamEvent::ToolCallPreview {
                    content_index: 11,
                    preview,
                },
            ),
            (
                ProviderEvent::ToolCallEnd {
                    content_index: 11,
                    tool_call: tool_call.clone(),
                },
                PublicStreamEvent::ToolCallEnd {
                    content_index: 11,
                    tool_call,
                },
            ),
            (
                ProviderEvent::ReasoningSummaryStart { content_index: 3 },
                PublicStreamEvent::ReasoningSummaryStart { content_index: 3 },
            ),
            (
                ProviderEvent::ReasoningSummaryDelta {
                    content_index: 3,
                    delta: "sum".to_owned(),
                },
                PublicStreamEvent::ReasoningSummaryDelta {
                    content_index: 3,
                    delta: "sum".to_owned(),
                },
            ),
            (
                ProviderEvent::ReasoningSummaryEnd {
                    content_index: 3,
                    content: "summary".to_owned(),
                },
                PublicStreamEvent::ReasoningSummaryEnd {
                    content_index: 3,
                    content: "summary".to_owned(),
                },
            ),
        ];
        for (provider, public) in cases {
            assert_eq!(
                projector.project(provider).expect("project event"),
                ProjectedProviderEvent::Update(AgentEvent::MessageUpdate {
                    message_id: "message-1".to_owned(),
                    event: public,
                })
            );
        }
    }

    #[test]
    fn rejected_tool_call_preserves_synthetic_result_outside_public_event() {
        let mut projector = started();
        let rejected = RejectedToolCall {
            id: "call-1".to_owned(),
            name: "read".to_owned(),
            error: ToolArgumentError::InvalidJson,
        };
        let synthetic_result = ToolResultMessage {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read".to_owned(),
            content: Vec::new(),
            details: json!({"error": "invalid"}),
            is_error: true,
            timestamp: Utc::now(),
        };
        assert_eq!(
            projector
                .project(ProviderEvent::ToolCallRejected {
                    content_index: 5,
                    rejected: rejected.clone(),
                    synthetic_result: synthetic_result.clone(),
                })
                .expect("rejection"),
            ProjectedProviderEvent::RejectedToolCall {
                event: AgentEvent::MessageUpdate {
                    message_id: "message-1".to_owned(),
                    event: PublicStreamEvent::ToolCallRejected {
                        content_index: 5,
                        rejected,
                    },
                },
                synthetic_result,
            }
        );
    }

    #[test]
    fn done_and_error_close_to_one_message_end_and_reject_any_suffix() {
        for (event, expected_kind) in [
            (
                ProviderEvent::Done {
                    reason: StopReason::Stop,
                    output: output(StopReason::Stop),
                },
                ProviderTerminalKind::Done,
            ),
            (
                ProviderEvent::Error {
                    reason: StopReason::Error,
                    output: output(StopReason::Error),
                },
                ProviderTerminalKind::Error,
            ),
        ] {
            let mut projector = started();
            let ProjectedProviderEvent::Terminal(terminal) =
                projector.project(event).expect("terminal")
            else {
                panic!("expected terminal");
            };
            assert_eq!(terminal.kind(), expected_kind);
            assert!(matches!(terminal.event(), AgentEvent::MessageEnd { .. }));
            assert_eq!(terminal.event().durable_kind(), Some("message_end"));
            assert!(
                projector
                    .project(ProviderEvent::TextStart { content_index: 0 })
                    .expect_err("terminal must fuse")
                    .to_string()
                    .contains("after terminal")
            );
        }
    }

    #[test]
    fn rejects_events_before_start_duplicate_start_and_malformed_terminal_kind() {
        let mut before = ProviderEventProjector::new("message-1").expect("projector");
        assert!(
            before
                .project(ProviderEvent::TextStart { content_index: 0 })
                .expect_err("data before Start")
                .to_string()
                .contains("before Start")
        );
        let mut duplicate = started();
        assert!(
            duplicate
                .project(ProviderEvent::Start)
                .expect_err("duplicate Start")
                .to_string()
                .contains("more than once")
        );
        let mut malformed = started();
        assert!(
            malformed
                .project(ProviderEvent::Done {
                    reason: StopReason::Error,
                    output: output(StopReason::Error),
                })
                .expect_err("Done Error")
                .to_string()
                .contains("Done cannot")
        );
    }

    #[test]
    fn t12_write_refuses_to_drop_provider_context() {
        let mut projector = started();
        let mut terminal_output = output(StopReason::Stop);
        terminal_output
            .provider_context
            .push(ProviderContextFragment {
                wire_item_index: Some(9),
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: json!({"encrypted_content": "opaque"}),
                },
            });
        let ProjectedProviderEvent::Terminal(terminal) = projector
            .project(ProviderEvent::Done {
                reason: StopReason::Stop,
                output: terminal_output,
            })
            .expect("terminal")
        else {
            panic!("expected terminal");
        };
        let error = match terminal.into_t12_write("run-1", "turn-1", true) {
            Ok(_) => panic!("context must not be dropped"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("T17"));
    }

    #[test]
    fn t12_write_couples_durable_message_end_and_projection() {
        let mut projector = started();
        let ProjectedProviderEvent::Terminal(terminal) = projector
            .project(ProviderEvent::Done {
                reason: StopReason::Stop,
                output: output(StopReason::Stop),
            })
            .expect("terminal")
        else {
            panic!("expected terminal");
        };
        let write = terminal
            .into_t12_write("run-1", "turn-1", true)
            .expect("T12 write");
        assert!(write.event.is_some());
        assert!(matches!(
            write.projections.as_slice(),
            [Projection::MessageEnd {
                message_id,
                role: "assistant",
                append_to_l0: true,
                ..
            }] if message_id == "message-1"
        ));
    }
}
