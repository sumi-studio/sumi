use std::collections::{HashMap, HashSet};

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    gateway::{Command, CommandAck},
    provider::types::{PublicAssistantContent, PublicMessage, StopReason, ToolResultMessage},
    runtime::contracts::ProcessGeneration,
    store::{
        ApplicationKind, ApprovalMutation, DurableEvent, EventBatch, EventWrite, EventWriter,
        InjectedCommand, Projection, RunPhase, ToolExecutionMutation,
    },
};

struct PendingSteerMessage {
    message_id: String,
    message: PublicMessage,
    barrier: MessageCommitBarrier,
}

use super::steer::{
    SteerGroup, SteerStage, finalize_hard_steer_batches, hard_steer_step_zero_batch,
    normalize_partial_assistant, steer_group_injection_batch,
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
    /// When a hard-steer close batch creates a new turn, the worker needs the
    /// durable turn identity to bind subsequent MessageStart/End events.
    pub new_turn_id: Option<String>,
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
    pending_tool_calls: HashSet<String>,
    length_not_started: HashSet<String>,
    pending_rejected_end: Option<(String, PublicMessage, HashSet<String>, MessageCommitBarrier)>,
    pending_rejected_results: Vec<(String, PublicMessage, MessageCommitBarrier)>,
    startup_agent_pending: bool,
    startup_turn_pending: bool,
    retry_wait_ready: bool,
    /// True when a retry-steer control was sent to the worker but the
    /// acceptance handshake did not complete.  Prevents a deferred command
    /// from being reclassified as a hard steer into the same retry attempt.
    retry_steer_accept_failed: bool,
    /// Steer group for tool/approval (soft) or retry-wait (retry) injection.
    pending_steer_group: Option<SteerGroup>,
    /// Buffered TurnEnd message/tool_results while waiting for a soft-steer group
    /// MessageEnd set to complete the injection batch.
    pending_steer_turn_end: Option<(PublicMessage, Vec<ToolResultMessage>)>,
    /// Buffered TurnStart while waiting for a soft-steer group MessageEnd set.
    pending_steer_turn_start: bool,
    /// True once the worker has emitted the Steered signal and we are collecting
    /// the matching MessageStart/End pairs.
    pending_steer_collecting: bool,
    /// Buffered MessageStart/End pairs for the steer group, in command order.
    pending_steer_messages: Vec<PendingSteerMessage>,
    /// The MessageStart that has arrived but not yet been paired.
    pending_steer_open_start: Option<(String, PublicMessage)>,
    /// Hard-steer steering command once step-0 classification has committed.
    pending_hard_steer: Option<AdmittedCommand>,
    /// Turn id assigned to the hard-steer command at step-0; reused when the
    /// close batch creates the matching TurnStart so the canonical EventBatch
    /// and the durable command row share one turn identity.
    pending_hard_steer_turn_id: Option<String>,
    /// Partial assistant message collected during a hard-steer cancellation.
    pending_hard_steer_partial: Option<(String, PublicMessage)>,
    /// The second batch of a hard-steer finalize, waiting for the worker to
    /// emit the matching user MessageStart/End pair.
    pending_hard_steer_inject_batch: Option<EventBatch>,
    /// The message id the worker must use for the hard-steer user message.
    pending_hard_steer_user_message_id: Option<String>,
    /// Barrier for the partial assistant MessageEnd that must be resolved when
    /// the close batch commits in `commit_hard_steer_start`.
    pending_hard_steer_message_barrier: Option<MessageCommitBarrier>,
    /// Original owner command cut off by an active Abort; applied alongside the
    /// final AgentEnd so the owner terminates with the aborted run.
    aborted_owner: Option<(String, u64)>,
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
            pending_tool_calls: HashSet::new(),
            length_not_started: HashSet::new(),
            pending_rejected_end: None,
            pending_rejected_results: Vec::new(),
            startup_agent_pending: false,
            startup_turn_pending: false,
            retry_wait_ready: false,
            retry_steer_accept_failed: false,
            pending_steer_group: None,
            pending_steer_turn_end: None,
            pending_steer_turn_start: false,
            pending_steer_collecting: false,
            pending_steer_messages: Vec::new(),
            pending_steer_open_start: None,
            pending_hard_steer: None,
            pending_hard_steer_turn_id: None,
            pending_hard_steer_partial: None,
            pending_hard_steer_inject_batch: None,
            pending_hard_steer_user_message_id: None,
            pending_hard_steer_message_barrier: None,
            aborted_owner: None,
            committed_terminal_command_ids: Vec::new(),
        }
    }

    pub(super) fn command_id(&self) -> &str {
        &self.binding.command_id
    }

    pub(super) fn steer_stage(&self) -> SteerStage {
        if self.retry_wait_ready {
            return SteerStage::RetryWait;
        }
        if self.assistant_open.is_some() {
            return SteerStage::AssistantGeneration;
        }
        if !self.pending_tool_end.is_empty() || !self.length_not_started.is_empty() {
            return SteerStage::ToolOrApproval;
        }
        SteerStage::Other
    }

    pub(super) fn can_bind_hard_steer(&self) -> bool {
        matches!(self.steer_stage(), SteerStage::AssistantGeneration)
            && self.pending_hard_steer.is_none()
            && self.pending_steer_group.is_none()
            && !self.retry_steer_accept_failed
    }

    pub(super) fn retry_steer_accept_failed(&self) -> bool {
        self.retry_steer_accept_failed
    }

    pub(super) fn set_retry_steer_accept_failed(&mut self, failed: bool) {
        self.retry_steer_accept_failed = failed;
    }

    pub(super) async fn bind_hard_steer(
        &mut self,
        writer: &EventWriter,
        command: AdmittedCommand,
    ) -> Result<()> {
        if !self.can_bind_hard_steer() {
            bail!("hard steer no longer matches an observable assistant generation");
        }
        if !matches!(command.envelope().command, Command::UserMessage { .. }) {
            bail!("hard steer requires a UserMessage");
        }
        let new_turn_id = Uuid::now_v7().to_string();
        writer
            .apply(hard_steer_step_zero_batch(
                &self.binding,
                &command,
                &new_turn_id,
            )?)
            .await?;
        self.pending_hard_steer = Some(command);
        self.pending_hard_steer_turn_id = Some(new_turn_id);
        Ok(())
    }

    pub(super) fn can_bind_abort(&self) -> bool {
        matches!(
            self.phase,
            RunPhase::UserStarted
                | RunPhase::UserCommitted
                | RunPhase::AssistantStarted
                | RunPhase::HardSteerRequested,
        )
    }

    pub(super) async fn bind_abort(
        &mut self,
        writer: &EventWriter,
        command: AdmittedCommand,
    ) -> Result<Vec<CommandAck>> {
        if !self.can_bind_abort() {
            bail!("abort no longer matches an active run boundary");
        }
        if !matches!(command.envelope().command, Command::Abort {}) {
            bail!("abort binding requires an Abort command");
        }

        let mut acks = writer
            .apply_active_abort_cutoff(
                command.envelope().command_id.as_str(),
                command.envelope().seq,
                &self.binding.run_id,
            )
            .await?;

        // The abort CommandApplied ACK is delayed until the worker emits AgentEnd,
        // so the Session sends only the earlier terminal ACKs (superseded/applied)
        // now and the final Applied ACK with the run's terminal events.
        acks.retain(|ack| ack.command_id != command.envelope().command_id.as_str());

        // The original owner completes with the aborted AgentEnd, not with the
        // abort control; record it now before the binding switches to Abort.
        self.aborted_owner = Some((self.worker_command_id.clone(), self.worker_command_seq));
        // Advance the bridge binding to the abort command itself so that the
        // final AgentEnd knows which control to ACK.
        self.binding.command_id = command.envelope().command_id.to_string();
        self.binding.command_seq = command.envelope().seq;
        self.phase = RunPhase::CancelRequested;

        Ok(acks)
    }

    pub(super) fn can_bind_soft_steer(
        &self,
        writer: &EventWriter,
        command: &AdmittedCommand,
    ) -> bool {
        let stage_ok = matches!(self.steer_stage(), SteerStage::ToolOrApproval)
            || (self.phase == RunPhase::AssistantStarted
                && self.assistant_open.is_none()
                && !self.turn_open
                && self.pending_tool_end.is_empty());
        if !stage_ok || !matches!(command.envelope().command, Command::UserMessage { .. }) {
            return false;
        }
        let redactor = writer.store().redactor();
        if let Some(group) = self.pending_steer_group.as_ref() {
            group.can_accept_with_size(
                command,
                &self.binding.run_id,
                group.turn_id(),
                ApplicationKind::SoftSteer,
                redactor,
            )
        } else {
            true
        }
    }

    pub(super) async fn bind_soft_steer(
        &mut self,
        writer: &EventWriter,
        command: AdmittedCommand,
    ) -> Result<()> {
        if !self.can_bind_soft_steer(writer, &command) {
            bail!(
                "soft steer no longer matches an observable tool/approval boundary or group is full"
            );
        }
        let redactor = writer.store().redactor();
        if let Some(group) = self.pending_steer_group.as_mut()
            && group.can_accept(
                &command,
                &self.binding.run_id,
                group.turn_id(),
                ApplicationKind::SoftSteer,
            )
        {
            let command_id = command.envelope().command_id.to_string();
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::CommandClassified {
                            command_id: command_id.clone(),
                            application_kind: ApplicationKind::SoftSteer,
                            run_id: self.binding.run_id.clone(),
                            turn_id: group.turn_id().to_owned(),
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await?;
            group.push(command, redactor)?;
            return Ok(());
        }
        let turn_id = Uuid::now_v7().to_string();
        let command_id = command.envelope().command_id.to_string();
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.clone(),
                        application_kind: ApplicationKind::SoftSteer,
                        run_id: self.binding.run_id.clone(),
                        turn_id: turn_id.clone(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await?;
        let mut group = SteerGroup::new(
            ApplicationKind::SoftSteer,
            self.binding.run_id.clone(),
            turn_id,
        )?;
        group.push(command, redactor)?;
        self.pending_steer_group = Some(group);
        Ok(())
    }

    pub(super) fn can_bind_retry_steer(
        &self,
        writer: &EventWriter,
        command: &AdmittedCommand,
    ) -> bool {
        if !matches!(command.envelope().command, Command::UserMessage { .. }) {
            return false;
        }
        if self.phase != RunPhase::AssistantStarted
            || !self.turn_open
            || self.assistant_open.is_some()
        {
            return false;
        }
        if let Some(group) = self.pending_steer_group.as_ref() {
            return group.can_accept_with_size(
                command,
                &self.binding.run_id,
                group.turn_id(),
                ApplicationKind::RetrySteer,
                writer.store().redactor(),
            );
        }
        self.retry_wait_ready
    }

    pub(super) async fn bind_retry_steer(
        &mut self,
        writer: &EventWriter,
        command: AdmittedCommand,
    ) -> Result<()> {
        if !self.can_bind_retry_steer(writer, &command) {
            bail!("retry steer no longer matches an observable retry wait or group is full");
        }
        let redactor = writer.store().redactor();
        if let Some(group) = self.pending_steer_group.as_mut()
            && group.can_accept(
                &command,
                &self.binding.run_id,
                group.turn_id(),
                ApplicationKind::RetrySteer,
            )
        {
            let command_id = command.envelope().command_id.to_string();
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::CommandClassified {
                            command_id: command_id.clone(),
                            application_kind: ApplicationKind::RetrySteer,
                            run_id: self.binding.run_id.clone(),
                            turn_id: group.turn_id().to_owned(),
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await?;
            group.push(command, redactor)?;
            return Ok(());
        }
        let turn_id = self.binding.turn_id.clone();
        let command_id = command.envelope().command_id.to_string();
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.clone(),
                        application_kind: ApplicationKind::RetrySteer,
                        run_id: self.binding.run_id.clone(),
                        turn_id: turn_id.clone(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await?;
        let mut group = SteerGroup::new(
            ApplicationKind::RetrySteer,
            self.binding.run_id.clone(),
            turn_id,
        )?;
        group.push(command, redactor)?;
        self.pending_steer_group = Some(group);
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
        let steer_group_active = self.pending_steer_group.is_some();
        let next_turn = matches!(output.event, AgentEvent::TurnStart)
            && !self.turn_open
            && self.phase != RunPhase::RunStarted
            && !steer_group_active;
        if next_turn {
            if output.binding.turn_id == self.binding.turn_id {
                bail!("next TurnStart reused the prior durable turn binding");
            }
        } else if !steer_group_active && output.binding.turn_id != self.binding.turn_id {
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
                if self.pending_steer_group.is_some() {
                    if self.pending_steer_collecting {
                        if self.pending_steer_open_start.is_some() {
                            bail!(
                                "a second steer group MessageStart arrived before its MessageEnd"
                            );
                        }
                        self.pending_steer_open_start = Some((message_id, *message));
                        Ok((Vec::new(), Vec::new()))
                    } else if matches!(message.as_ref(), PublicMessage::User(_)) {
                        // The first user MessageStart of a soft/retry group begins collection.
                        self.pending_steer_collecting = true;
                        self.pending_steer_open_start = Some((message_id, *message));
                        Ok((Vec::new(), Vec::new()))
                    } else {
                        bail!("steer group MessageStart must be a user message");
                    }
                } else {
                    if self.pending_start.is_some() {
                        bail!("a second non-assistant MessageStart arrived before its MessageEnd");
                    }
                    self.pending_start = Some((message_id, *message));
                    Ok((Vec::new(), Vec::new()))
                }
            }
            AgentEvent::MessageEnd {
                message_id,
                message,
            } if !matches!(message.as_ref(), PublicMessage::Assistant(_)) => {
                if self.pending_steer_group.is_some() && self.pending_steer_collecting {
                    self.collect_steer_group_message_end(
                        writer,
                        message_id,
                        *message,
                        message_commit_barrier.expect("MessageEnd barrier checked"),
                    )
                    .await
                } else if self.pending_hard_steer_inject_batch.is_some() {
                    self.commit_hard_steer_user(
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
                if self.turn_open && self.pending_steer_group.is_none() {
                    bail!("TurnStart arrived while the prior turn remained open");
                }
                if let Some(group) = &self.pending_steer_group {
                    if self.pending_steer_turn_start {
                        bail!("duplicate buffered TurnStart for steer group");
                    }
                    if self.pending_steer_turn_end.is_none()
                        && group.application_kind() == ApplicationKind::SoftSteer
                    {
                        bail!("soft-steer TurnStart arrived without a buffered TurnEnd");
                    }
                    self.pending_steer_turn_start = true;
                    self.binding.turn_id = output.binding.turn_id;
                    self.turn_open = true;
                    Ok((Vec::new(), Vec::new()))
                } else {
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
                } else if self.phase == RunPhase::AssistantStarted
                    || self.phase == RunPhase::CancelRequested
                {
                    Vec::new()
                } else {
                    bail!(
                        "assistant MessageStart requires a committed or aborted user owner (phase {})",
                        self.phase.as_str()
                    );
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
                self.pending_tool_calls.remove(&tool_call_id);
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
                if self.pending_steer_group.is_some()
                    && self
                        .pending_steer_group
                        .as_ref()
                        .unwrap()
                        .application_kind()
                        == ApplicationKind::SoftSteer
                    && self.pending_steer_turn_end.is_none()
                {
                    self.pending_steer_turn_end = Some(((*message).clone(), tool_results.clone()));
                    Ok((Vec::new(), Vec::new()))
                } else {
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
            }
            AgentEvent::AgentEnd => {
                if self.turn_open {
                    bail!("AgentEnd requires the current TurnEnd to be durable first");
                }
                let abort_cutoff = self.phase == RunPhase::CancelRequested;
                self.phase = RunPhase::Finished;
                let mut projections = Vec::with_capacity(2);
                if abort_cutoff {
                    if let Some((owner_id, owner_seq)) = self.aborted_owner.take() {
                        projections.push(Projection::CommandApplied {
                            command_id: owner_id,
                            command_seq: owner_seq,
                            run_id: Some(self.binding.run_id.clone()),
                        });
                    }
                } else {
                    projections.push(Projection::CommandApplied {
                        command_id: self.binding.command_id.clone(),
                        command_seq: self.binding.command_seq,
                        run_id: Some(self.binding.run_id.clone()),
                    });
                }
                self.commit_single(
                    writer,
                    DurableEvent::agent_end(&self.binding.run_id)?,
                    projections,
                    AgentEvent::AgentEnd,
                )
                .await
            }
            AgentEvent::Steered {
                mode: super::SteerMode::Hard,
            } if self.pending_hard_steer_inject_batch.is_some() => {
                // The close batch was already applied when the partial
                // assistant MessageEnd committed; the inject batch follows
                // when the worker emits the matching user MessageEnd.
                Ok((Vec::new(), Vec::new()))
            }
            AgentEvent::Steered {
                mode: super::SteerMode::Soft,
            } if self.pending_steer_group.is_some() => {
                if self.pending_steer_collecting {
                    bail!("duplicate Steered signal for steer group");
                }
                self.pending_steer_collecting = true;
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
        self.pending_tool_calls.clear();
        let mut rejected = HashSet::new();
        if let PublicMessage::Assistant(assistant) = &message {
            let is_length = assistant.stop_reason == StopReason::Length
                || (assistant.stop_reason == StopReason::Error
                    && assistant.provider_code.as_deref() == Some(LENGTH_LOOP_CODE));
            for item in &assistant.content {
                match item {
                    PublicAssistantContent::ToolCall { tool_call, .. } if is_length => {
                        self.length_not_started.insert(tool_call.id.clone());
                    }
                    PublicAssistantContent::ToolCall { tool_call, .. } => {
                        self.pending_tool_calls.insert(tool_call.id.clone());
                    }
                    PublicAssistantContent::RejectedToolCall { rejected: r, .. } => {
                        rejected.insert(r.id.clone());
                    }
                    _ => {}
                }
            }
        }
        let append_to_l0 = !matches!(
            &message,
            PublicMessage::Assistant(assistant) if assistant.stop_reason == StopReason::Error
        );
        if self.pending_hard_steer.is_some()
            && matches!(
                &message,
                PublicMessage::Assistant(assistant)
                    if assistant.stop_reason == StopReason::Aborted && assistant.interrupted
            )
        {
            return self
                .commit_hard_steer_partial(writer, message_id, message, barrier)
                .await;
        }
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
            None,
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
            } else if self.pending_tool_calls.remove(&tool_call_id) {
                if !result.is_error {
                    bail!("Cancelled ToolResult must be is_error=true");
                }
                projections.push(Projection::ToolExecution(ToolExecutionMutation::Skip {
                    tool_call_id: tool_call_id.clone(),
                    command_id: self.binding.command_id.clone(),
                    run_id: self.binding.run_id.clone(),
                    turn_id: self.binding.turn_id.clone(),
                    executor_generation: self.binding.executor_generation,
                    idempotency_key: self.binding.tool_execution_idempotency_key(&tool_call_id),
                    error_code: "user_steer_cancelled",
                }));
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
            None,
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
            None,
        )
        .await
    }

    async fn commit_message_batch(
        &mut self,
        writer: &EventWriter,
        batch: EventBatch,
        public: Vec<AgentEvent>,
        receipt_requests: Vec<(String, MessageCommitBarrier)>,
        new_turn_id: Option<String>,
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
                    new_turn_id: new_turn_id.clone(),
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

    async fn commit_hard_steer_partial(
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
            .pending_hard_steer
            .take()
            .ok_or_else(|| anyhow!("hard-steer partial MessageEnd has no bound command"))?;
        let normalized = normalize_partial_assistant(message.clone())
            .map_err(|error| anyhow!("hard-steer partial normalization failed: {error}"))?;
        let new_turn_id = self
            .pending_hard_steer_turn_id
            .take()
            .ok_or_else(|| anyhow!("hard-steer partial MessageEnd has no pending turn id"))?;
        let mut batches = finalize_hard_steer_batches(
            &self.binding,
            &command,
            message_id.clone(),
            message,
            &new_turn_id,
        )?;
        if batches.len() != 2 {
            bail!("finalize_hard_steer_batches must return exactly two EventBatch");
        }
        let inject_batch = batches.remove(1);
        let close_batch = batches.remove(0);

        let close_seqs = writer.apply(close_batch).await?;
        if close_seqs.len() != 2 {
            bail!("hard-steer close batch did not commit exactly two durable events");
        }
        let partial_message_seq = close_seqs[0];
        barrier.resolve(MessageCommitReceipt {
            message_id: message_id.clone(),
            message_seq: partial_message_seq,
            new_turn_id: Some(new_turn_id.clone()),
        });

        let close_public = vec![
            AgentEvent::MessageEnd {
                message_id: message_id.clone(),
                message: Box::new(normalized.clone()),
            },
            AgentEvent::TurnEnd {
                message: Some(Box::new(normalized)),
                tool_results: Vec::new(),
            },
        ];
        let outputs = close_public
            .into_iter()
            .zip(close_seqs)
            .map(|(event, seq)| CommittedOutput {
                event,
                seq: Some(seq),
            })
            .collect();

        self.binding.turn_id = new_turn_id;
        self.turn_open = true;
        self.assistant_open = None;
        self.pending_hard_steer_inject_batch = Some(inject_batch);
        self.pending_hard_steer_user_message_id = Some(crate::store::user_message_id(
            &command.envelope().command_id,
        ));
        self.pending_hard_steer = Some(command);
        Ok((outputs, Vec::new()))
    }

    async fn commit_hard_steer_user(
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
            .pending_hard_steer
            .take()
            .ok_or_else(|| anyhow!("hard-steer user MessageEnd has no bound command"))?;
        let inject_batch = self
            .pending_hard_steer_inject_batch
            .take()
            .ok_or_else(|| anyhow!("hard-steer user MessageEnd has no pending inject batch"))?;
        let expected_message_id = self
            .pending_hard_steer_user_message_id
            .take()
            .ok_or_else(|| anyhow!("hard-steer user MessageEnd has no expected message id"))?;
        if message_id != expected_message_id {
            bail!("hard-steer user message id does not derive from the steering command");
        }

        let Command::UserMessage { text, attachments } = &command.envelope().command else {
            bail!("hard-steer command changed kind before user injection");
        };
        if !attachments.is_empty() {
            bail!("T16 hard steer does not accept attachments");
        }
        let expected_message = PublicMessage::User(crate::provider::types::UserMessage {
            content: vec![crate::provider::types::UserContent::Text { text: text.clone() }],
            timestamp: command.received_at(),
        });
        if message != expected_message {
            bail!("hard-steer user message does not match durable command plaintext");
        }

        let seqs = writer.apply(inject_batch).await?;
        if seqs.len() != 4 {
            bail!("hard-steer inject batch did not commit exactly four durable events");
        }
        let user_message_seq = seqs[3];
        let inject_public = vec![
            AgentEvent::Steered {
                mode: super::SteerMode::Hard,
            },
            AgentEvent::TurnStart,
            AgentEvent::MessageStart {
                message_id: message_id.clone(),
                message: Box::new(message.clone()),
            },
            AgentEvent::MessageEnd {
                message_id: message_id.clone(),
                message: Box::new(message),
            },
        ];
        self.binding.command_id = command.envelope().command_id.to_string();
        self.binding.command_seq = command.envelope().seq;
        self.phase = RunPhase::UserCommitted;
        Ok((
            inject_public
                .into_iter()
                .zip(seqs)
                .map(|(event, seq)| CommittedOutput {
                    event,
                    seq: Some(seq),
                })
                .collect(),
            vec![(
                barrier,
                MessageCommitReceipt {
                    message_id,
                    message_seq: user_message_seq,
                    new_turn_id: None,
                },
            )],
        ))
    }

    async fn collect_steer_group_message_end(
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
            .pending_steer_open_start
            .take()
            .ok_or_else(|| anyhow!("steer group MessageEnd has no MessageStart"))?;
        if start_id != message_id || start_message != message {
            bail!("steer group MessageEnd does not match its MessageStart");
        }
        let group_len = self
            .pending_steer_group
            .as_ref()
            .map(|group| group.len())
            .ok_or_else(|| anyhow!("steer group MessageEnd has no bound group"))?;
        let index = self.pending_steer_messages.len();
        if index >= group_len {
            bail!("steer group MessageEnd exceeds group size");
        }
        let command = self
            .pending_steer_group
            .as_ref()
            .and_then(|group| group.commands().get(index))
            .ok_or_else(|| anyhow!("steer group member disappeared before MessageEnd"))?;
        let expected_message_id = crate::store::user_message_id(&command.envelope().command_id);
        if message_id != expected_message_id {
            bail!("steer group message identity does not derive from its command");
        }
        let expected_message = super::steer::build_user_message(command)?;
        if message != expected_message {
            bail!("steer group message does not match durable command plaintext");
        }
        self.pending_steer_messages.push(PendingSteerMessage {
            message_id,
            message,
            barrier,
        });
        if self.pending_steer_messages.len() == group_len {
            return self.commit_steer_group(writer).await;
        }
        Ok((Vec::new(), Vec::new()))
    }

    async fn commit_steer_group(
        &mut self,
        writer: &EventWriter,
    ) -> Result<(
        Vec<CommittedOutput>,
        Vec<(MessageCommitBarrier, MessageCommitReceipt)>,
    )> {
        let group = self
            .pending_steer_group
            .take()
            .ok_or_else(|| anyhow!("steer group commit has no bound group"))?;
        let previous_owner = self.binding.clone();
        let (closing_turn_message, closing_tool_results) =
            self.pending_steer_turn_end.take().unzip();
        let closing_tool_results = closing_tool_results.unwrap_or_default();
        let _ = self.pending_steer_turn_start; // consumed by the snapshot
        let messages = std::mem::take(&mut self.pending_steer_messages);
        self.pending_steer_collecting = false;
        self.pending_steer_open_start = None;
        self.pending_steer_turn_start = false;

        let is_soft = group.application_kind() == ApplicationKind::SoftSteer;
        let commands = group.commands().to_vec();
        let group_len = commands.len();
        let group_turn_id = group.turn_id().to_owned();
        let mut snapshot = group.snapshot(previous_owner, closing_turn_message.clone());
        snapshot.closing_tool_results = closing_tool_results.clone();
        let batch = steer_group_injection_batch(snapshot)?;
        self.collect_terminal_command_ids(&batch);
        let seqs = writer.apply(batch).await?;

        let expected_writes = if is_soft {
            1 + group_len + 1 + 2 * group_len
        } else {
            group_len + 2 * group_len
        };
        if seqs.len() != expected_writes {
            bail!(
                "steer group injection committed {} durable events, expected {}",
                seqs.len(),
                expected_writes
            );
        }

        // Build public events in batch order.
        let mut public = Vec::with_capacity(expected_writes);
        let mut seq_iter = seqs.into_iter();
        if is_soft {
            let turn_end_seq = seq_iter.next().unwrap();
            public.push(CommittedOutput {
                event: AgentEvent::TurnEnd {
                    message: closing_turn_message.map(Box::new),
                    tool_results: closing_tool_results.clone(),
                },
                seq: Some(turn_end_seq),
            });
        }
        for _ in 0..group_len {
            let seq = seq_iter.next().unwrap();
            public.push(CommittedOutput {
                event: AgentEvent::Steered {
                    mode: super::SteerMode::Soft,
                },
                seq: Some(seq),
            });
        }
        if is_soft {
            let turn_start_seq = seq_iter.next().unwrap();
            public.push(CommittedOutput {
                event: AgentEvent::TurnStart,
                seq: Some(turn_start_seq),
            });
        }
        let message_start_base = public.len();
        for (index, command) in commands.iter().enumerate() {
            let user_message = super::steer::build_user_message(command)?;
            let user_message_id = crate::store::user_message_id(&command.envelope().command_id);
            let start_seq = seq_iter.next().unwrap();
            let end_seq = seq_iter.next().unwrap();
            public.push(CommittedOutput {
                event: AgentEvent::MessageStart {
                    message_id: user_message_id.clone(),
                    message: Box::new(user_message.clone()),
                },
                seq: Some(start_seq),
            });
            public.push(CommittedOutput {
                event: AgentEvent::MessageEnd {
                    message_id: user_message_id,
                    message: Box::new(user_message),
                },
                seq: Some(end_seq),
            });
            // Receipts are resolved in command order.
            let pending = messages.get(index).ok_or_else(|| {
                anyhow!("steer group committed without a matching MessageEnd receipt request")
            })?;
            let expected_id = crate::store::user_message_id(&command.envelope().command_id);
            if pending.message_id != expected_id {
                bail!("steer group receipt message id mismatch");
            }
        }

        // Resolve all MessageEnd barriers with their durable seqs.
        let mut receipts = Vec::with_capacity(messages.len());
        for (index, pending) in messages.into_iter().enumerate() {
            let end_position = message_start_base + 2 * index + 1;
            let message_seq = public[end_position]
                .seq
                .ok_or_else(|| anyhow!("steer group MessageEnd has no seq"))?;
            receipts.push((
                pending.barrier,
                MessageCommitReceipt {
                    message_id: pending.message_id,
                    message_seq,
                    new_turn_id: None,
                },
            ));
        }

        // Update durable ownership to the last group member.
        if let Some(last) = commands.last() {
            self.binding.command_id = last.envelope().command_id.to_string();
            self.binding.command_seq = last.envelope().seq;
            self.binding.turn_id = group_turn_id;
        }
        self.phase = RunPhase::UserCommitted;
        self.turn_open = true;
        self.retry_wait_ready = false;

        Ok((public, receipts))
    }

    async fn commit_single(
        &mut self,
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

    fn collect_terminal_command_ids(&mut self, batch: &EventBatch) {
        for write in &batch.writes {
            for projection in &write.projections {
                if let Projection::CommandApplied { command_id, .. } = projection
                    && !self.committed_terminal_command_ids.contains(command_id)
                {
                    self.committed_terminal_command_ids.push(command_id.clone());
                }
            }
        }
    }

    async fn commit_batch(
        &mut self,
        writer: &EventWriter,
        batch: EventBatch,
        public: Vec<AgentEvent>,
    ) -> Result<Vec<CommittedOutput>> {
        self.collect_terminal_command_ids(&batch);
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
