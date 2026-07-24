use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    gateway::Command,
    provider::types::{PublicAssistantContent, PublicMessage, StopReason},
    runtime::contracts::ProcessGeneration,
    store::{
        ApplicationKind, ApprovalMutation, DurableEvent, EventBatch, EventWrite, EventWriter,
        InjectedCommand, Projection, RunPhase, ToolExecutionMutation,
    },
};

use super::{AdmittedCommand, AgentEvent, ApprovalResolution, run::LENGTH_LOOP_CODE};

#[derive(Clone, Debug)]
pub(crate) struct DurableRunBinding {
    pub command_id: String,
    pub command_seq: u64,
    pub run_id: String,
    pub turn_id: String,
    pub executor_generation: ProcessGeneration,
}

impl DurableRunBinding {
    pub(super) fn idle(command: &AdmittedCommand, executor_generation: ProcessGeneration) -> Self {
        Self {
            command_id: command.envelope().command_id.to_string(),
            command_seq: command.envelope().seq,
            run_id: Uuid::now_v7().to_string(),
            turn_id: Uuid::now_v7().to_string(),
            executor_generation,
        }
    }

    fn tool_execution_idempotency_key(&self, tool_call_id: &str) -> String {
        format!("{}/{tool_call_id}", self.command_id)
    }
}

/// Private, metadata-bound worker output. Public events deliberately carry no
/// durable identities; this value binds them before EventWriter sees them.
pub(crate) struct RunOutput {
    pub binding: DurableRunBinding,
    pub event: AgentEvent,
    pub commit_barrier: Option<ToolStartCommitBarrier>,
    pub message_commit_barrier: Option<MessageCommitBarrier>,
    pub retry_wait_commit_barrier: Option<RetryWaitCommitBarrier>,
}

impl RunOutput {
    pub(super) fn detached(
        binding: DurableRunBinding,
        event: AgentEvent,
        commit_barrier: Option<ToolStartCommitBarrier>,
    ) -> Self {
        let message_commit_barrier = matches!(event, AgentEvent::MessageEnd { .. }).then(|| {
            let (barrier, receiver) = MessageCommitBarrier::channel();
            drop(receiver);
            barrier
        });
        let retry_wait_commit_barrier =
            matches!(event, AgentEvent::RetryScheduled { .. }).then(|| {
                let (barrier, receiver) = RetryWaitCommitBarrier::channel();
                drop(receiver);
                barrier
            });
        Self {
            binding,
            event,
            commit_barrier,
            message_commit_barrier,
            retry_wait_commit_barrier,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MessageCommitReceipt {
    pub message_id: String,
    pub message_seq: u64,
}

pub(crate) struct MessageCommitBarrier(oneshot::Sender<MessageCommitReceipt>);

impl MessageCommitBarrier {
    pub(crate) fn channel() -> (Self, oneshot::Receiver<MessageCommitReceipt>) {
        let (sender, receiver) = oneshot::channel();
        (Self(sender), receiver)
    }

    pub(crate) fn resolve(self, receipt: MessageCommitReceipt) {
        let _ = self.0.send(receipt);
    }
}

pub(crate) struct ToolStartCommitBarrier(oneshot::Sender<()>);

impl ToolStartCommitBarrier {
    pub(crate) fn channel() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        (Self(sender), receiver)
    }

    pub(super) fn committed(self) {
        let _ = self.0.send(());
    }
}

pub(crate) struct RetryWaitCommitBarrier(oneshot::Sender<()>);

impl RetryWaitCommitBarrier {
    pub(crate) fn channel() -> (Self, oneshot::Receiver<()>) {
        let (sender, receiver) = oneshot::channel();
        (Self(sender), receiver)
    }

    pub(super) fn committed(self) {
        let _ = self.0.send(());
    }
}

pub(super) struct CommittedRunOutput {
    pub outputs: Vec<CommittedOutput>,
    pub tool_start_barrier: Option<ToolStartCommitBarrier>,
    message_receipts: Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    retry_wait_commit_barrier: Option<RetryWaitCommitBarrier>,
    terminal_command_ids: Vec<String>,
}

impl CommittedRunOutput {
    pub(super) fn resolve_message_receipts(
        self,
    ) -> (
        Vec<CommittedOutput>,
        Option<ToolStartCommitBarrier>,
        Option<RetryWaitCommitBarrier>,
        Vec<String>,
    ) {
        for (barrier, receipt) in self.message_receipts {
            barrier.resolve(receipt);
        }
        (
            self.outputs,
            self.tool_start_barrier,
            self.retry_wait_commit_barrier,
            self.terminal_command_ids,
        )
    }
}

pub(super) struct CommittedOutput {
    pub event: AgentEvent,
    pub seq: Option<u64>,
}

pub(super) struct DurableBridge {
    binding: DurableRunBinding,
    worker_command_id: String,
    worker_command_seq: u64,
    phase: RunPhase,
    turn_open: bool,
    assistant_open: Option<String>,
    pending_start: Option<(String, PublicMessage)>,
    pending_tool_end: HashMap<String, (Value, bool)>,
    length_not_started: HashSet<String>,
    pending_rejected_end: Option<(String, PublicMessage, HashSet<String>, MessageCommitBarrier)>,
    pending_rejected_results: Vec<(String, PublicMessage, MessageCommitBarrier)>,
    startup_agent_pending: bool,
    startup_turn_pending: bool,
    retry_wait_ready: bool,
    pending_retry_steer: Option<AdmittedCommand>,
    retry_steered: bool,
    committed_terminal_command_ids: Vec<String>,
}

impl DurableBridge {
    pub(super) fn new(binding: DurableRunBinding) -> Self {
        let worker_command_id = binding.command_id.clone();
        let worker_command_seq = binding.command_seq;
        Self {
            binding,
            worker_command_id,
            worker_command_seq,
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
            retry_wait_ready: false,
            pending_retry_steer: None,
            retry_steered: false,
            committed_terminal_command_ids: Vec::new(),
        }
    }

    pub(super) fn command_id(&self) -> &str {
        &self.binding.command_id
    }

    pub(super) fn can_bind_retry_steer(&self) -> bool {
        self.retry_wait_ready
            && self.pending_retry_steer.is_none()
            && self.phase == RunPhase::AssistantStarted
            && self.turn_open
            && self.assistant_open.is_none()
    }

    pub(super) async fn bind_retry_steer(
        &mut self,
        writer: &EventWriter,
        command: AdmittedCommand,
    ) -> Result<()> {
        if !self.can_bind_retry_steer() {
            bail!("retry steer no longer matches an observable retry wait");
        }
        if !matches!(command.envelope().command, Command::UserMessage { .. }) {
            bail!("retry steer requires a UserMessage");
        }
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command.envelope().command_id.to_string(),
                        application_kind: ApplicationKind::RetrySteer,
                        run_id: self.binding.run_id.clone(),
                        turn_id: self.binding.turn_id.clone(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await?;
        self.pending_retry_steer = Some(command);
        self.retry_wait_ready = false;
        Ok(())
    }

    pub(super) async fn commit(
        &mut self,
        writer: &EventWriter,
        output: RunOutput,
    ) -> Result<CommittedRunOutput> {
        let has_barrier = output.commit_barrier.is_some();
        let is_tool_start = matches!(output.event, AgentEvent::ToolExecutionStart { .. });
        if has_barrier != is_tool_start {
            bail!("commit barrier is required exactly for ToolExecutionStart");
        }
        let is_message_end = matches!(output.event, AgentEvent::MessageEnd { .. });
        if output.message_commit_barrier.is_some() != is_message_end {
            bail!("message commit barrier is required exactly for MessageEnd");
        }
        let is_retry_scheduled = matches!(output.event, AgentEvent::RetryScheduled { .. });
        if output.retry_wait_commit_barrier.is_some() != is_retry_scheduled {
            bail!("retry-wait commit barrier is required exactly for RetryScheduled");
        }
        if output.binding.command_id != self.worker_command_id
            || output.binding.command_seq != self.worker_command_seq
            || output.binding.run_id != self.binding.run_id
            || output.binding.executor_generation != self.binding.executor_generation
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
        let message_commit_barrier = output.message_commit_barrier;
        let outputs = match event {
            AgentEvent::MessageUpdate { ref message_id, .. } => {
                if self.assistant_open.as_deref() != Some(message_id.as_str()) {
                    bail!("volatile message update has no prerequisite durable MessageStart");
                }
                Ok((vec![CommittedOutput { event, seq: None }], Vec::new()))
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
                Ok((vec![CommittedOutput { event, seq: None }], Vec::new()))
            }
            AgentEvent::Error { .. } => {
                Ok((vec![CommittedOutput { event, seq: None }], Vec::new()))
            }
            AgentEvent::MessageStart {
                message_id,
                message,
            } if !matches!(message.as_ref(), PublicMessage::Assistant(_)) => {
                if self.pending_start.is_some() {
                    bail!("a second non-assistant MessageStart arrived before its MessageEnd");
                }
                self.pending_start = Some((message_id, *message));
                Ok((Vec::new(), Vec::new()))
            }
            AgentEvent::MessageEnd {
                message_id,
                message,
            } if !matches!(message.as_ref(), PublicMessage::Assistant(_)) => {
                if self.retry_steered {
                    self.commit_retry_steer(
                        writer,
                        message_id,
                        *message,
                        message_commit_barrier.expect("MessageEnd barrier checked"),
                    )
                    .await
                } else {
                    self.commit_non_assistant(
                        writer,
                        message_id,
                        *message,
                        message_commit_barrier.expect("MessageEnd barrier checked"),
                    )
                    .await
                }
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
                Ok((Vec::new(), Vec::new()))
            }
            AgentEvent::AgentStart => {
                if self.phase != RunPhase::Classified || self.startup_agent_pending {
                    bail!("AgentStart does not match the classified idle startup");
                }
                self.phase = RunPhase::RunStarted;
                self.startup_agent_pending = true;
                Ok((Vec::new(), Vec::new()))
            }
            AgentEvent::TurnStart => {
                if self.turn_open {
                    bail!("TurnStart arrived while the prior turn remained open");
                }
                if self.phase == RunPhase::RunStarted {
                    self.phase = RunPhase::TurnStarted;
                    self.startup_turn_pending = true;
                    self.turn_open = true;
                    Ok((Vec::new(), Vec::new()))
                } else {
                    self.binding.turn_id = output.binding.turn_id;
                    self.turn_open = true;
                    self.commit_single(
                        writer,
                        DurableEvent::turn_start(&self.binding.run_id, &self.binding.turn_id)?,
                        Vec::new(),
                        AgentEvent::TurnStart,
                    )
                    .await
                }
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
                self.retry_wait_ready = false;
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
                self.commit_assistant_end(
                    writer,
                    message_id,
                    *message,
                    message_commit_barrier.expect("MessageEnd barrier checked"),
                )
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
                                    executor_generation: self.binding.executor_generation,
                                    idempotency_key: self
                                        .binding
                                        .tool_execution_idempotency_key(&tool_call_id),
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
                let committed = self
                    .commit_single(
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
                    .await?;
                self.retry_wait_ready = delay_ms > 0;
                Ok(committed)
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
            AgentEvent::Steered {
                mode: super::SteerMode::Soft,
            } if self.pending_retry_steer.is_some() && !self.retry_steered => {
                self.retry_steered = true;
                Ok((Vec::new(), Vec::new()))
            }
            AgentEvent::ApprovalRequested { request } => {
                if self.phase != RunPhase::AssistantStarted
                    || !self.turn_open
                    || self.assistant_open.is_some()
                {
                    bail!(
                        "ApprovalRequested requires a durable assistant tool call in the active turn"
                    );
                }
                let request_id = request.id.clone();
                let tool_call_id = request.tool_call_id.clone();
                self.commit_single(
                    writer,
                    DurableEvent::approval_requested(request.clone())?,
                    vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: tool_call_id.clone(),
                            command_id: self.binding.command_id.clone(),
                            run_id: self.binding.run_id.clone(),
                            executor_generation: self.binding.executor_generation,
                            idempotency_key: self
                                .binding
                                .tool_execution_idempotency_key(&tool_call_id),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id,
                            tool_call_id,
                            run_id: self.binding.run_id.clone(),
                            turn_id: self.binding.turn_id.clone(),
                        }),
                    ],
                    AgentEvent::ApprovalRequested { request },
                )
                .await
            }
            AgentEvent::ApprovalResolved {
                request_id,
                resolution: ApprovalResolution::Cancelled,
            } => {
                self.commit_single(
                    writer,
                    DurableEvent::approval_resolved(
                        request_id.clone(),
                        ApprovalResolution::Cancelled,
                        "runtime".to_owned(),
                    )?,
                    vec![Projection::Approval(ApprovalMutation::Resolve {
                        request_id: request_id.clone(),
                        state: "cancelled",
                        actor: "runtime".to_owned(),
                    })],
                    AgentEvent::ApprovalResolved {
                        request_id,
                        resolution: ApprovalResolution::Cancelled,
                    },
                )
                .await
            }
            AgentEvent::ApprovalResolved { .. } => {
                bail!("user approval decisions require the later T23 ApprovalBroker")
            }
            AgentEvent::Steered { .. } | AgentEvent::MemoryMaintenance { .. } => {
                bail!("event requires a later T15/T16/T17 durable bridge extension")
            }
        }?;
        let (outputs, message_receipts) = outputs;
        Ok(CommittedRunOutput {
            outputs,
            tool_start_barrier: output.commit_barrier,
            message_receipts,
            retry_wait_commit_barrier: output.retry_wait_commit_barrier,
            terminal_command_ids: std::mem::take(&mut self.committed_terminal_command_ids),
        })
    }

    async fn commit_retry_steer(
        &mut self,
        writer: &EventWriter,
        message_id: String,
        message: PublicMessage,
        barrier: MessageCommitBarrier,
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
        let command = self
            .pending_retry_steer
            .take()
            .ok_or_else(|| anyhow!("Steered has no bound retry command"))?;
        let (start_id, start_message) = self
            .pending_start
            .take()
            .ok_or_else(|| anyhow!("retry steer MessageEnd has no MessageStart"))?;
        if start_id != message_id || start_message != message {
            bail!("retry steer MessageEnd does not match its exact MessageStart");
        }
        let expected_message_id = crate::store::user_message_id(&command.envelope().command_id);
        if message_id != expected_message_id {
            bail!("retry steer message identity does not derive from its command");
        }
        let Command::UserMessage { text, attachments } = &command.envelope().command else {
            bail!("retry steer command changed kind before injection");
        };
        if !attachments.is_empty() {
            bail!("T15 retry steer does not accept attachments");
        }
        let PublicMessage::User(user) = &message else {
            bail!("retry steer must inject a user message");
        };
        let expected =
            crate::provider::types::PublicMessage::User(crate::provider::types::UserMessage {
                content: vec![crate::provider::types::UserContent::Text { text: text.clone() }],
                timestamp: command.received_at(),
            });
        if message != expected || user.content.is_empty() {
            bail!("retry steer message does not match durable command plaintext");
        }

        let command_id = command.envelope().command_id.to_string();
        let command_seq = command.envelope().seq;
        let run_id = self.binding.run_id.clone();
        let turn_id = self.binding.turn_id.clone();
        let previous_owner_command_id = self.binding.command_id.clone();
        let previous_owner_command_seq = self.binding.command_seq;
        let steered = AgentEvent::Steered {
            mode: super::SteerMode::Soft,
        };
        let start = AgentEvent::MessageStart {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        };
        let end = AgentEvent::MessageEnd {
            message_id: message_id.clone(),
            message: Box::new(message.clone()),
        };
        let sequences = writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(DurableEvent::steered(
                            super::SteerMode::Soft,
                            command_id.clone(),
                            run_id.clone(),
                            turn_id.clone(),
                        )?),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.clone(),
                            run_id: run_id.clone(),
                            expected: RunPhase::Classified,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(DurableEvent::message(
                            "message_start",
                            &message_id,
                            &message,
                        )?),
                        projections: vec![
                            Projection::CommandApplied {
                                command_id: previous_owner_command_id.clone(),
                                command_seq: previous_owner_command_seq,
                                run_id: Some(run_id.clone()),
                            },
                            Projection::RunPhase {
                                command_id: command_id.clone(),
                                run_id: run_id.clone(),
                                expected: RunPhase::TurnStarted,
                                next: RunPhase::UserStarted,
                            },
                        ],
                    },
                    EventWrite {
                        event: Some(DurableEvent::message("message_end", &message_id, &message)?),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: message_id.clone(),
                                role: "user",
                                message: message.clone(),
                                append_to_l0: true,
                            },
                            Projection::RunPhase {
                                command_id: command_id.clone(),
                                run_id: run_id.clone(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(
                    command_seq,
                    command.envelope().command_id.clone(),
                )],
            })
            .await?;
        if sequences.len() != 3 {
            bail!("retry steer injection did not commit exactly three durable events");
        }
        self.binding.command_id = command_id;
        self.binding.command_seq = command_seq;
        self.phase = RunPhase::UserCommitted;
        self.retry_steered = false;
        self.committed_terminal_command_ids
            .push(previous_owner_command_id);
        let message_seq = sequences[2];
        let outputs = [steered, start, end]
            .into_iter()
            .zip(sequences)
            .map(|(event, seq)| CommittedOutput {
                event,
                seq: Some(seq),
            })
            .collect();
        Ok((
            outputs,
            vec![(
                barrier,
                MessageCommitReceipt {
                    message_id,
                    message_seq,
                },
            )],
        ))
    }

    async fn commit_assistant_end(
        &mut self,
        writer: &EventWriter,
        message_id: String,
        message: PublicMessage,
        barrier: MessageCommitBarrier,
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
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
            self.pending_rejected_end = Some((message_id, message, rejected, barrier));
            return Ok((Vec::new(), Vec::new()));
        }
        self.commit_message_batch(
            writer,
            EventBatch {
                writes: vec![EventWrite {
                    event: Some(DurableEvent::message_in_turn(
                        "message_end",
                        &message_id,
                        &message,
                        Some(self.binding.run_id.clone()),
                        Some(self.binding.turn_id.clone()),
                    )?),
                    projections: vec![Projection::MessageEnd {
                        message_id: message_id.clone(),
                        role: "assistant",
                        message: message.clone(),
                        append_to_l0,
                    }],
                }],
                injected_commands: Vec::new(),
            },
            vec![AgentEvent::MessageEnd {
                message_id: message_id.clone(),
                message: Box::new(message),
            }],
            vec![(message_id, barrier)],
        )
        .await
    }

    async fn commit_non_assistant(
        &mut self,
        writer: &EventWriter,
        message_id: String,
        message: PublicMessage,
        barrier: MessageCommitBarrier,
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
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
                    executor_generation: self.binding.executor_generation,
                    idempotency_key: self.binding.tool_execution_idempotency_key(&tool_call_id),
                    error_code: "length_guard",
                }));
            } else if let Some((_, _, pending_ids, _)) = self.pending_rejected_end.as_mut()
                && pending_ids.remove(&tool_call_id)
            {
                if !result.is_error {
                    bail!("RejectedToolCall synthetic ToolResult must be is_error=true");
                }
                self.pending_rejected_results
                    .push((message_id.clone(), message.clone(), barrier));
                if !pending_ids.is_empty() {
                    return Ok((Vec::new(), Vec::new()));
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
                message_id: message_id.clone(),
                message: Box::new(message),
            },
        ]);
        self.commit_message_batch(
            writer,
            EventBatch {
                writes,
                injected_commands,
            },
            public_prefix,
            vec![(message_id, barrier)],
        )
        .await
    }

    async fn commit_rejected_pair_batch(
        &mut self,
        writer: &EventWriter,
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
        let (assistant_id, assistant, pending_ids, assistant_barrier) = self
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
        let mut receipt_requests = vec![(assistant_id.clone(), assistant_barrier)];
        let mut public = vec![AgentEvent::MessageEnd {
            message_id: assistant_id,
            message: Box::new(assistant),
        }];
        for (message_id, message, barrier) in self.pending_rejected_results.drain(..) {
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
                message_id: message_id.clone(),
                message: Box::new(message),
            });
            receipt_requests.push((message_id, barrier));
        }
        self.commit_message_batch(
            writer,
            EventBatch {
                writes,
                injected_commands: Vec::new(),
            },
            public,
            receipt_requests,
        )
        .await
    }

    async fn commit_message_batch(
        &self,
        writer: &EventWriter,
        batch: EventBatch,
        public: Vec<AgentEvent>,
        receipt_requests: Vec<(String, MessageCommitBarrier)>,
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
        let outputs = self.commit_batch(writer, batch, public).await?;
        let mut committed_by_id = HashMap::new();
        for output in &outputs {
            if let AgentEvent::MessageEnd { message_id, .. } = &output.event {
                let seq = output
                    .seq
                    .ok_or_else(|| anyhow!("MessageEnd commit has no durable seq"))?;
                if committed_by_id.insert(message_id.clone(), seq).is_some() {
                    bail!("atomic batch committed duplicate MessageEnd identity");
                }
            }
        }
        if committed_by_id.len() != receipt_requests.len() {
            bail!("atomic batch MessageEnd receipt cardinality mismatch");
        }
        let mut receipts = Vec::with_capacity(receipt_requests.len());
        for (message_id, barrier) in receipt_requests {
            let message_seq = committed_by_id.remove(&message_id).ok_or_else(|| {
                anyhow!("atomic batch omitted receipt-bound MessageEnd {message_id}")
            })?;
            receipts.push((
                barrier,
                MessageCommitReceipt {
                    message_id,
                    message_seq,
                },
            ));
        }
        if !committed_by_id.is_empty() {
            bail!("atomic batch contained an unbound MessageEnd receipt");
        }
        Ok((outputs, receipts))
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
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
        let outputs = self
            .commit_batch(
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
            .await?;
        Ok((outputs, Vec::new()))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(command_id: &str) -> DurableRunBinding {
        DurableRunBinding {
            command_id: command_id.to_owned(),
            command_seq: 1,
            run_id: "run-a".to_owned(),
            turn_id: "turn-a".to_owned(),
            executor_generation: ProcessGeneration::from_wire(73).unwrap(),
        }
    }

    #[test]
    fn tool_execution_identity_is_exactly_command_slash_call_and_ignores_run_turn() {
        let first = binding("command-a");
        assert_eq!(
            first.tool_execution_idempotency_key("call-a"),
            "command-a/call-a"
        );

        let mut different_run_and_turn = first.clone();
        different_run_and_turn.run_id = "run-b".to_owned();
        different_run_and_turn.turn_id = "turn-b".to_owned();
        assert_eq!(
            first.tool_execution_idempotency_key("call-a"),
            different_run_and_turn.tool_execution_idempotency_key("call-a")
        );
        assert_ne!(
            first.tool_execution_idempotency_key("call-a"),
            binding("command-b").tool_execution_idempotency_key("call-a")
        );
        assert_ne!(
            first.tool_execution_idempotency_key("call-a"),
            first.tool_execution_idempotency_key("call-b")
        );
    }

    #[test]
    fn executor_generation_remains_private_run_output_metadata() {
        let output = RunOutput {
            binding: binding("command-a"),
            event: AgentEvent::AgentStart,
            commit_barrier: None,
            message_commit_barrier: None,
            retry_wait_commit_barrier: None,
        };
        assert_eq!(output.binding.executor_generation.to_wire(), 73);
        let public = serde_json::to_value(output.event).expect("serialize public event");
        assert_eq!(public, serde_json::json!({"type":"agent_start"}));
        assert!(public.get("executor_generation").is_none());
    }
}
