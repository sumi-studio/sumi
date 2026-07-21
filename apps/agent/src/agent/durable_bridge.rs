use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    provider::types::{PublicAssistantContent, PublicMessage, StopReason},
    store::{
        DurableEvent, EventBatch, EventWrite, EventWriter, InjectedCommand, Projection, RunPhase,
        ToolExecutionMutation,
    },
};

use super::{AdmittedCommand, AgentEvent, run::LENGTH_LOOP_CODE};

#[derive(Clone, Debug)]
pub(crate) struct DurableRunBinding {
    pub command_id: String,
    pub command_seq: u64,
    pub run_id: String,
    pub turn_id: String,
}

impl DurableRunBinding {
    pub(super) fn idle(command: &AdmittedCommand) -> Self {
        Self {
            command_id: command.envelope().command_id.to_string(),
            command_seq: command.envelope().seq,
            run_id: Uuid::now_v7().to_string(),
            turn_id: Uuid::now_v7().to_string(),
        }
    }
}

/// Private, metadata-bound worker output. Public events deliberately carry no
/// durable identities; this value binds them before EventWriter sees them.
pub(crate) struct RunOutput {
    pub binding: DurableRunBinding,
    pub event: AgentEvent,
}

pub(super) struct CommittedOutput {
    pub event: AgentEvent,
    pub seq: Option<u64>,
}

pub(super) struct DurableBridge {
    binding: DurableRunBinding,
    phase: RunPhase,
    turn_open: bool,
    assistant_open: Option<String>,
    pending_start: Option<(String, PublicMessage)>,
    pending_tool_end: HashMap<String, (Value, bool)>,
    length_not_started: HashSet<String>,
    pending_rejected_end: Option<(String, PublicMessage, HashSet<String>)>,
    pending_rejected_results: Vec<(String, PublicMessage)>,
    startup_agent_pending: bool,
    startup_turn_pending: bool,
}

impl DurableBridge {
    pub(super) fn new(binding: DurableRunBinding) -> Self {
        Self {
            binding,
            phase: RunPhase::Classified,
            turn_open: false,
            assistant_open: None,
            pending_start: None,
            pending_tool_end: HashMap::new(),
            length_not_started: HashSet::new(),
            pending_rejected_end: None,
            pending_rejected_results: Vec::new(),
            startup_agent_pending: false,
            startup_turn_pending: false,
        }
    }

    pub(super) fn command_id(&self) -> &str {
        &self.binding.command_id
    }

    pub(super) async fn commit(
        &mut self,
        writer: &EventWriter,
        output: RunOutput,
    ) -> Result<Vec<CommittedOutput>> {
        if output.binding.command_id != self.binding.command_id
            || output.binding.command_seq != self.binding.command_seq
            || output.binding.run_id != self.binding.run_id
        {
            bail!("run output durable binding changed while the worker was active");
        }
        let next_turn = matches!(output.event, AgentEvent::TurnStart)
            && !self.turn_open
            && self.phase != RunPhase::RunStarted;
        if next_turn {
            if output.binding.turn_id == self.binding.turn_id {
                bail!("next TurnStart reused the prior durable turn binding");
            }
        } else if output.binding.turn_id != self.binding.turn_id {
            bail!("run output durable turn binding changed outside TurnStart");
        }
        let event = output.event;
        match event {
            AgentEvent::MessageUpdate { ref message_id, .. } => {
                if self.assistant_open.as_deref() != Some(message_id.as_str()) {
                    bail!("volatile message update has no prerequisite durable MessageStart");
                }
                Ok(vec![CommittedOutput { event, seq: None }])
            }
            AgentEvent::ToolExecutionUpdate {
                ref tool_call_id, ..
            } => {
                match self.pending_tool_end.get(tool_call_id) {
                    Some((result, _)) if result.is_null() => {}
                    Some(_) => bail!("volatile tool update arrived after ToolExecutionEnd"),
                    None => {
                        bail!("volatile tool update has no prerequisite durable ToolExecutionStart")
                    }
                }
                Ok(vec![CommittedOutput { event, seq: None }])
            }
            AgentEvent::Error { .. } => Ok(vec![CommittedOutput { event, seq: None }]),
            AgentEvent::MessageStart {
                message_id,
                message,
            } if !matches!(message.as_ref(), PublicMessage::Assistant(_)) => {
                if self.pending_start.is_some() {
                    bail!("a second non-assistant MessageStart arrived before its MessageEnd");
                }
                self.pending_start = Some((message_id, *message));
                Ok(Vec::new())
            }
            AgentEvent::MessageEnd {
                message_id,
                message,
            } if !matches!(message.as_ref(), PublicMessage::Assistant(_)) => {
                self.commit_non_assistant(writer, message_id, *message)
                    .await
            }
            AgentEvent::ToolExecutionEnd {
                tool_call_id,
                result,
                is_error,
            } => {
                let pending = self
                    .pending_tool_end
                    .get_mut(&tool_call_id)
                    .ok_or_else(|| anyhow!("ToolExecutionEnd has no durable ToolExecutionStart"))?;
                if !pending.0.is_null() {
                    bail!("duplicate pending ToolExecutionEnd");
                }
                *pending = (result, is_error);
                Ok(Vec::new())
            }
            AgentEvent::AgentStart => {
                if self.phase != RunPhase::Classified || self.startup_agent_pending {
                    bail!("AgentStart does not match the classified idle startup");
                }
                self.phase = RunPhase::RunStarted;
                self.startup_agent_pending = true;
                Ok(Vec::new())
            }
            AgentEvent::TurnStart => {
                if self.turn_open {
                    bail!("TurnStart arrived while the prior turn remained open");
                }
                if self.phase == RunPhase::RunStarted {
                    self.phase = RunPhase::TurnStarted;
                    self.startup_turn_pending = true;
                    self.turn_open = true;
                    return Ok(Vec::new());
                } else {
                    self.binding.turn_id = output.binding.turn_id;
                }
                self.turn_open = true;
                self.commit_single(
                    writer,
                    DurableEvent::turn_start(&self.binding.run_id, &self.binding.turn_id)?,
                    Vec::new(),
                    AgentEvent::TurnStart,
                )
                .await
            }
            AgentEvent::MessageStart {
                message_id,
                message,
            } => {
                if !self.turn_open || self.assistant_open.is_some() {
                    bail!(
                        "assistant MessageStart requires one exact open turn and no open attempt"
                    );
                }
                let projections = if self.phase == RunPhase::UserCommitted {
                    vec![self.transition(RunPhase::UserCommitted, RunPhase::AssistantStarted)?]
                } else if self.phase == RunPhase::AssistantStarted {
                    Vec::new()
                } else {
                    bail!("assistant MessageStart requires a committed user owner");
                };
                self.assistant_open = Some(message_id.clone());
                self.commit_single(
                    writer,
                    DurableEvent::message_in_turn(
                        "message_start",
                        &message_id,
                        &message,
                        Some(self.binding.run_id.clone()),
                        Some(self.binding.turn_id.clone()),
                    )?,
                    projections,
                    AgentEvent::MessageStart {
                        message_id,
                        message,
                    },
                )
                .await
            }
            AgentEvent::MessageEnd {
                message_id,
                message,
            } => {
                self.commit_assistant_end(writer, message_id, *message)
                    .await
            }
            AgentEvent::ToolExecutionStart {
                tool_call_id,
                tool_name,
                args,
            } => {
                if self.assistant_open.is_some() {
                    bail!("ToolExecutionStart cannot precede assistant MessageEnd");
                }
                writer
                    .apply(EventBatch {
                        writes: vec![EventWrite {
                            event: None,
                            projections: vec![Projection::ToolExecution(
                                ToolExecutionMutation::Prepare {
                                    tool_call_id: tool_call_id.clone(),
                                    command_id: self.binding.command_id.clone(),
                                    run_id: self.binding.run_id.clone(),
                                    executor_generation: 0,
                                    idempotency_key: format!(
                                        "{}:{}",
                                        self.binding.run_id, tool_call_id
                                    ),
                                },
                            )],
                        }],
                        injected_commands: Vec::new(),
                    })
                    .await?;
                self.pending_tool_end
                    .insert(tool_call_id.clone(), (Value::Null, false));
                self.commit_single(
                    writer,
                    DurableEvent::tool_execution_start(
                        tool_call_id.clone(),
                        tool_name.clone(),
                        args.clone(),
                    )?,
                    vec![Projection::ToolExecution(ToolExecutionMutation::Start {
                        tool_call_id: tool_call_id.clone(),
                        run_id: self.binding.run_id.clone(),
                    })],
                    AgentEvent::ToolExecutionStart {
                        tool_call_id,
                        tool_name,
                        args,
                    },
                )
                .await
            }
            AgentEvent::RetryScheduled {
                attempt,
                delay_ms,
                retry_at,
                error_message,
            } => {
                self.commit_single(
                    writer,
                    DurableEvent::retry_scheduled(
                        &self.binding.run_id,
                        &self.binding.turn_id,
                        attempt,
                        delay_ms,
                        retry_at,
                        error_message.clone(),
                    )?,
                    Vec::new(),
                    AgentEvent::RetryScheduled {
                        attempt,
                        delay_ms,
                        retry_at,
                        error_message,
                    },
                )
                .await
            }
            AgentEvent::TurnEnd {
                message,
                tool_results,
            } => {
                let message =
                    message.ok_or_else(|| anyhow!("T15 run cannot emit empty TurnEnd"))?;
                self.turn_open = false;
                self.commit_single(
                    writer,
                    DurableEvent::turn_end(
                        &self.binding.run_id,
                        &self.binding.turn_id,
                        (*message).clone(),
                        tool_results.clone(),
                    )?,
                    Vec::new(),
                    AgentEvent::TurnEnd {
                        message: Some(message),
                        tool_results,
                    },
                )
                .await
            }
            AgentEvent::AgentEnd => {
                if self.turn_open {
                    bail!("AgentEnd requires the current TurnEnd to be durable first");
                }
                self.phase = RunPhase::Finished;
                self.commit_single(
                    writer,
                    DurableEvent::agent_end(&self.binding.run_id)?,
                    vec![Projection::CommandApplied {
                        command_id: self.binding.command_id.clone(),
                        command_seq: self.binding.command_seq,
                        run_id: Some(self.binding.run_id.clone()),
                    }],
                    AgentEvent::AgentEnd,
                )
                .await
            }
            AgentEvent::Steered { .. }
            | AgentEvent::ApprovalRequested { .. }
            | AgentEvent::ApprovalResolved { .. }
            | AgentEvent::MemoryMaintenance { .. } => {
                bail!("event requires a later T15/T16/T17 durable bridge extension")
            }
        }
    }

    async fn commit_assistant_end(
        &mut self,
        writer: &EventWriter,
        message_id: String,
        message: PublicMessage,
    ) -> Result<Vec<CommittedOutput>> {
        if self.assistant_open.as_deref() != Some(message_id.as_str()) {
            bail!("assistant MessageEnd does not close its exact durable MessageStart");
        }
        self.assistant_open = None;
        self.length_not_started.clear();
        let mut rejected = HashSet::new();
        if let PublicMessage::Assistant(assistant) = &message {
            if assistant.stop_reason == StopReason::Length
                || (assistant.stop_reason == StopReason::Error
                    && assistant.provider_code.as_deref() == Some(LENGTH_LOOP_CODE))
            {
                self.length_not_started
                    .extend(assistant.content.iter().filter_map(|item| match item {
                        PublicAssistantContent::ToolCall { tool_call, .. } => {
                            Some(tool_call.id.clone())
                        }
                        _ => None,
                    }));
            }
            rejected.extend(assistant.content.iter().filter_map(|item| match item {
                PublicAssistantContent::RejectedToolCall { rejected, .. } => {
                    Some(rejected.id.clone())
                }
                _ => None,
            }));
        }
        let append_to_l0 = !matches!(
            &message,
            PublicMessage::Assistant(assistant) if assistant.stop_reason == StopReason::Error
        );
        if !rejected.is_empty() {
            if self.pending_rejected_end.is_some() || !self.pending_rejected_results.is_empty() {
                bail!("a rejected assistant pair is already pending");
            }
            self.pending_rejected_end = Some((message_id, message, rejected));
            return Ok(Vec::new());
        }
        self.commit_single(
            writer,
            DurableEvent::message_in_turn(
                "message_end",
                &message_id,
                &message,
                Some(self.binding.run_id.clone()),
                Some(self.binding.turn_id.clone()),
            )?,
            vec![Projection::MessageEnd {
                message_id: message_id.clone(),
                role: "assistant",
                message: message.clone(),
                append_to_l0,
            }],
            AgentEvent::MessageEnd {
                message_id,
                message: Box::new(message),
            },
        )
        .await
    }

    async fn commit_non_assistant(
        &mut self,
        writer: &EventWriter,
        message_id: String,
        message: PublicMessage,
    ) -> Result<Vec<CommittedOutput>> {
        let (start_id, start_message) = self
            .pending_start
            .take()
            .ok_or_else(|| anyhow!("non-assistant MessageEnd has no buffered MessageStart"))?;
        if start_id != message_id || start_message != message {
            bail!("non-assistant MessageStart/End pair is not exact");
        }
        let mut projections = vec![Projection::MessageEnd {
            message_id: message_id.clone(),
            role: match &message {
                PublicMessage::User(_) => "user",
                PublicMessage::ToolResult(_) => "tool_result",
                PublicMessage::Assistant(_) => unreachable!(),
            },
            message: message.clone(),
            append_to_l0: true,
        }];
        let mut writes = Vec::new();
        let mut public_prefix = Vec::new();
        let mut injected_commands = Vec::new();
        if matches!(message, PublicMessage::User(_)) {
            if self.startup_agent_pending || self.startup_turn_pending {
                if !(self.startup_agent_pending && self.startup_turn_pending) {
                    bail!("idle startup lifecycle is only partially buffered");
                }
                writes.push(EventWrite {
                    event: Some(DurableEvent::agent_start(&self.binding.run_id)?),
                    projections: vec![Projection::RunPhase {
                        command_id: self.binding.command_id.clone(),
                        run_id: self.binding.run_id.clone(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                });
                writes.push(EventWrite {
                    event: Some(DurableEvent::turn_start(
                        &self.binding.run_id,
                        &self.binding.turn_id,
                    )?),
                    projections: vec![Projection::RunPhase {
                        command_id: self.binding.command_id.clone(),
                        run_id: self.binding.run_id.clone(),
                        expected: RunPhase::RunStarted,
                        next: RunPhase::TurnStarted,
                    }],
                });
                public_prefix.extend([AgentEvent::AgentStart, AgentEvent::TurnStart]);
                self.startup_agent_pending = false;
                self.startup_turn_pending = false;
            }
            projections.push(self.transition(RunPhase::TurnStarted, RunPhase::UserStarted)?);
            projections.push(self.transition(RunPhase::UserStarted, RunPhase::UserCommitted)?);
            injected_commands.push(InjectedCommand::new(
                self.binding.command_seq,
                crate::gateway::CommandId::parse(&self.binding.command_id)
                    .map_err(anyhow::Error::msg)?,
            ));
        } else if let PublicMessage::ToolResult(result) = &message {
            let tool_call_id = result.tool_call_id.clone();
            if let Some((result_value, is_error)) = self.pending_tool_end.remove(&tool_call_id) {
                if result_value.is_null() {
                    bail!("tool result arrived before ToolExecutionEnd");
                }
                if result_value != serde_json::to_value(result)? || is_error != result.is_error {
                    bail!("ToolExecutionEnd and ToolResult message disagree");
                }
                let state = if is_error { "failed" } else { "succeeded" };
                let error_code = is_error.then_some("executor_failed");
                writes.push(EventWrite {
                    event: Some(DurableEvent::tool_execution_end(
                        tool_call_id.clone(),
                        result_value,
                        is_error,
                        state.to_owned(),
                        error_code.map(str::to_owned),
                    )?),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                        tool_call_id,
                        expected: "running",
                        state,
                        error_code,
                    })],
                });
                public_prefix.push(AgentEvent::ToolExecutionEnd {
                    tool_call_id: result.tool_call_id.clone(),
                    result: serde_json::to_value(result)?,
                    is_error,
                });
            } else if self.length_not_started.remove(&tool_call_id) {
                if !result.is_error {
                    bail!("Length-not-started ToolResult must be is_error=true");
                }
                projections.push(Projection::ToolExecution(ToolExecutionMutation::Skip {
                    tool_call_id: tool_call_id.clone(),
                    command_id: self.binding.command_id.clone(),
                    run_id: self.binding.run_id.clone(),
                    turn_id: self.binding.turn_id.clone(),
                    idempotency_key: format!("{}:{tool_call_id}:not-started", self.binding.run_id),
                    error_code: "length_guard",
                }));
            } else if let Some((_, _, pending_ids)) = self.pending_rejected_end.as_mut()
                && pending_ids.remove(&tool_call_id)
            {
                if !result.is_error {
                    bail!("RejectedToolCall synthetic ToolResult must be is_error=true");
                }
                self.pending_rejected_results
                    .push((message_id.clone(), message.clone()));
                if !pending_ids.is_empty() {
                    return Ok(Vec::new());
                }
                return self.commit_rejected_pair_batch(writer).await;
            } else {
                bail!(
                    "tool result has neither execution lifecycle nor Length not-started disposition"
                );
            }
        }
        writes.push(EventWrite {
            event: Some(DurableEvent::message(
                "message_start",
                &message_id,
                &message,
            )?),
            projections: Vec::new(),
        });
        writes.push(EventWrite {
            event: Some(DurableEvent::message("message_end", &message_id, &message)?),
            projections,
        });
        public_prefix.extend([
            AgentEvent::MessageStart {
                message_id: message_id.clone(),
                message: Box::new(message.clone()),
            },
            AgentEvent::MessageEnd {
                message_id,
                message: Box::new(message),
            },
        ]);
        self.commit_batch(
            writer,
            EventBatch {
                writes,
                injected_commands,
            },
            public_prefix,
        )
        .await
    }

    async fn commit_rejected_pair_batch(
        &mut self,
        writer: &EventWriter,
    ) -> Result<Vec<CommittedOutput>> {
        let (assistant_id, assistant, pending_ids) = self
            .pending_rejected_end
            .take()
            .ok_or_else(|| anyhow!("rejected result has no pending assistant"))?;
        if !pending_ids.is_empty() {
            bail!("rejected assistant pair is incomplete");
        }
        let append_to_l0 = !matches!(
            &assistant,
            PublicMessage::Assistant(value) if value.stop_reason == StopReason::Error
        );
        let mut writes = vec![EventWrite {
            event: Some(DurableEvent::message_in_turn(
                "message_end",
                &assistant_id,
                &assistant,
                Some(self.binding.run_id.clone()),
                Some(self.binding.turn_id.clone()),
            )?),
            projections: vec![Projection::MessageEnd {
                message_id: assistant_id.clone(),
                role: "assistant",
                message: assistant.clone(),
                append_to_l0,
            }],
        }];
        let mut public = vec![AgentEvent::MessageEnd {
            message_id: assistant_id,
            message: Box::new(assistant),
        }];
        for (message_id, message) in self.pending_rejected_results.drain(..) {
            writes.push(EventWrite {
                event: Some(DurableEvent::message(
                    "message_start",
                    &message_id,
                    &message,
                )?),
                projections: Vec::new(),
            });
            writes.push(EventWrite {
                event: Some(DurableEvent::message("message_end", &message_id, &message)?),
                projections: vec![Projection::MessageEnd {
                    message_id: message_id.clone(),
                    role: "tool_result",
                    message: message.clone(),
                    append_to_l0: true,
                }],
            });
            public.push(AgentEvent::MessageStart {
                message_id: message_id.clone(),
                message: Box::new(message.clone()),
            });
            public.push(AgentEvent::MessageEnd {
                message_id,
                message: Box::new(message),
            });
        }
        self.commit_batch(
            writer,
            EventBatch {
                writes,
                injected_commands: Vec::new(),
            },
            public,
        )
        .await
    }

    fn transition(&mut self, expected: RunPhase, next: RunPhase) -> Result<Projection> {
        if self.phase != expected {
            bail!(
                "durable bridge expected phase {}, found {}",
                expected.as_str(),
                self.phase.as_str()
            );
        }
        self.phase = next;
        Ok(Projection::RunPhase {
            command_id: self.binding.command_id.clone(),
            run_id: self.binding.run_id.clone(),
            expected,
            next,
        })
    }

    async fn commit_single(
        &self,
        writer: &EventWriter,
        durable: DurableEvent,
        projections: Vec<Projection>,
        public: AgentEvent,
    ) -> Result<Vec<CommittedOutput>> {
        self.commit_batch(
            writer,
            EventBatch {
                writes: vec![EventWrite {
                    event: Some(durable),
                    projections,
                }],
                injected_commands: Vec::new(),
            },
            vec![public],
        )
        .await
    }

    async fn commit_batch(
        &self,
        writer: &EventWriter,
        batch: EventBatch,
        public: Vec<AgentEvent>,
    ) -> Result<Vec<CommittedOutput>> {
        let seqs = writer.apply(batch).await?;
        if seqs.len() != public.len() {
            bail!("durable EventBatch/public event cardinality mismatch");
        }
        Ok(public
            .into_iter()
            .zip(seqs)
            .map(|(event, seq)| CommittedOutput {
                event,
                seq: Some(seq),
            })
            .collect())
    }
}
