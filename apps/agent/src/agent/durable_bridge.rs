use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::Value;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    approval::{ExecutableGrant, GrantLease, GrantRevalidation},
    gateway::{ApprovalDecision, Command, CommandAck},
    memory::estimate::eviction_footprint_for_payload,
    provider::{
        model::ModelSpec,
        types::{
            ProviderContextFragment, PublicAssistantContent, PublicMessage, StopReason,
            ToolResultMessage,
        },
    },
    runtime::contracts::ProcessGeneration,
    store::{
        ApplicationKind, ApprovalMutation, ApprovalRuleMutation, DurableEvent,
        ErrorContextDisposition, EventBatch, EventWrite, EventWriter, InjectedCommand, Projection,
        RunPhase, ToolExecutionMutation,
    },
};

struct PendingSteerMessage {
    message_id: String,
    message: PublicMessage,
    barrier: MessageCommitBarrier,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingErrorProviderContext {
    message_id: String,
    message_seq: u64,
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
    /// For `ApprovalResolved` decisions, the authenticated `AdmittedCommand`
    /// whose `CommandApplied` projection must accompany the resolution.
    pub approval_command: Option<AdmittedCommand>,
    /// Tool-call ID whose `ToolResult` should be durably skipped as
    /// approval-denied before any matching `ToolExecutionStart`.
    pub approval_not_started: Option<String>,
    /// Tool-call ID whose `ToolResult` should be durably skipped as
    /// approval-cancelled before any matching `ToolExecutionStart`.
    pub approval_cancelled: Option<String>,
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
            approval_command: None,
            approval_not_started: None,
            approval_cancelled: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MessageCommitReceipt {
    pub message_id: String,
    pub message_seq: u64,
    /// Exact big-endian IEEE-754 bits committed by EventWriter for the
    /// prompt-token calibration observation carried by this MessageEnd.
    pub calibration_ratio_bits: Option<[u8; 8]>,
    /// When a hard-steer close batch creates a new turn, the worker needs the
    /// durable turn identity to bind subsequent MessageStart/End events.
    pub new_turn_id: Option<String>,
}

pub(crate) struct MessageCommitBarrier {
    sender: oneshot::Sender<MessageCommitReceipt>,
    provider_context: Vec<ProviderContextFragment>,
    calibration_estimate: Option<u64>,
}

impl MessageCommitBarrier {
    pub(crate) fn channel() -> (Self, oneshot::Receiver<MessageCommitReceipt>) {
        Self::channel_with_provider_context(Vec::new())
    }

    pub(crate) fn channel_with_provider_context(
        provider_context: Vec<ProviderContextFragment>,
    ) -> (Self, oneshot::Receiver<MessageCommitReceipt>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender,
                provider_context,
                calibration_estimate: None,
            },
            receiver,
        )
    }

    pub(crate) fn channel_with_provider_context_and_calibration(
        provider_context: Vec<ProviderContextFragment>,
        uncalibrated_prompt_estimate: u64,
    ) -> (Self, oneshot::Receiver<MessageCommitReceipt>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender,
                provider_context,
                calibration_estimate: Some(uncalibrated_prompt_estimate),
            },
            receiver,
        )
    }

    fn provider_context(&self) -> &[ProviderContextFragment] {
        &self.provider_context
    }

    fn calibration_estimate(&self) -> Option<u64> {
        self.calibration_estimate
    }

    #[cfg(test)]
    pub(crate) fn provider_context_for_test(&self) -> &[ProviderContextFragment] {
        self.provider_context()
    }

    pub(crate) fn resolve(self, receipt: MessageCommitReceipt) {
        let _ = self.sender.send(receipt);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ToolStartCommitResult {
    Committed,
    Reauthorize,
}

pub(crate) struct ToolStartCommitBarrier {
    sender: oneshot::Sender<ToolStartCommitResult>,
    grant: Option<ExecutableGrant>,
}

impl ToolStartCommitBarrier {
    pub(crate) fn channel() -> (Self, oneshot::Receiver<ToolStartCommitResult>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender,
                grant: None,
            },
            receiver,
        )
    }

    pub(crate) fn channel_with_grant(
        grant: ExecutableGrant,
    ) -> (Self, oneshot::Receiver<ToolStartCommitResult>) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender,
                grant: Some(grant),
            },
            receiver,
        )
    }

    async fn revalidate_grant(
        &self,
        tool_call_id: &str,
        tool_name: &str,
        args: &Value,
        run_id: &str,
        turn_id: &str,
    ) -> Result<(GrantRevalidation, GrantLease<'_>)> {
        match self.grant.as_ref() {
            Some(grant) => {
                grant
                    .authorize(tool_call_id, tool_name, args, run_id, turn_id)
                    .await
            }
            None => Ok((GrantRevalidation::Valid, GrantLease::empty())),
        }
    }

    pub(super) fn committed(self) {
        let _ = self.sender.send(ToolStartCommitResult::Committed);
    }

    fn reauthorize(self) {
        let _ = self.sender.send(ToolStartCommitResult::Reauthorize);
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
    /// Authoritative Error context remains durable only until the first
    /// attempt disposition. That disposition prepares its fixed Invalidate
    /// intent; the bridge then fences progress while the common applier
    /// destroys the rows and per-item key.
    pending_error_provider_context: Option<PendingErrorProviderContext>,
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
    /// Tool-call IDs that were approval-denied and must be durably skipped as
    /// `approval_denied` when their `ToolResult` arrives, before any matching
    /// `ToolExecutionStart`. Inserted by `ApprovalResolved` Deny; removed when
    /// the skip/finish projection is committed.
    approval_not_started: HashSet<String>,
    /// Tool-call IDs that were approval-cancelled and must be durably finished
    /// as `approval_cancelled` when their `ToolResult` arrives. Inserted by
    /// `ApprovalResolved` Cancelled or idle cancellation; removed when the finish
    /// projection is committed.
    approval_cancelled: HashSet<String>,
    /// The authenticated `ApprovalDecision` command for the currently pending
    /// approval. Set from the worker's `RunOutput` when `ApprovalResolved` is
    /// emitted and consumed when the Decision is committed so the matching
    /// `CommandApplied` projection can be emitted. At most one command is kept.
    approval_command: Option<AdmittedCommand>,
    /// Maps an active approval `request_id` to the `tool_call_id` it controls.
    /// Inserted when `ApprovalRequested` commits; consumed by the matching
    /// `ApprovalResolved` to locate the affected tool.
    approval_request_tools: HashMap<String, String>,
    /// Tool-call IDs that have been prepared for approval and are waiting for
    /// an `ApprovalResolved` before they may start execution. Inserted when
    /// `ApprovalRequested` commits; removed when the tool starts (then it is
    /// running) or when the approval is denied/cancelled and cleaned up.
    approval_prepared_tools: HashSet<String>,
    /// Approved approvals that have resolved but whose tools have not yet started.
    /// Each tuple holds the `request_id`, the `ApprovalResolution`, and the
    /// authenticated `ApprovalDecision` command. Entries are pushed by
    /// `ApprovalResolved` Decision (ApproveOnce/ApproveAlways) and consumed in
    /// order by the matching `ToolExecutionStart`, which emits the
    /// `ApprovalResolved` and `CommandApplied` projections atomically. `AgentEnd`
    /// cannot commit while this queue is non-empty.
    pending_approval_resolved: Vec<(String, ApprovalResolution, AdmittedCommand)>,
    /// Approved decisions preempted before their tool start. The matching
    /// runtime cancellation, prepared-tool terminal, ToolResult, and this
    /// authenticated command's terminal no-op commit together when the worker
    /// supplies the cancellation result.
    pending_cancelled_approval_commands: HashMap<String, (String, AdmittedCommand)>,
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
            pending_error_provider_context: None,
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
            approval_not_started: HashSet::new(),
            approval_cancelled: HashSet::new(),
            approval_command: None,
            approval_request_tools: HashMap::new(),
            approval_prepared_tools: HashSet::new(),
            pending_approval_resolved: Vec::new(),
            pending_cancelled_approval_commands: HashMap::new(),
            committed_terminal_command_ids: Vec::new(),
        }
    }

    pub(super) fn command_id(&self) -> &str {
        &self.binding.command_id
    }

    fn provider_context_footprint(
        message: &PublicMessage,
        provider_context: &[ProviderContextFragment],
    ) -> Result<u64> {
        let PublicMessage::Assistant(assistant) = message else {
            bail!("provider context may only accompany an assistant message");
        };
        let spec = ModelSpec::from_origin(&assistant.origin)
            .ok_or_else(|| anyhow!("no canonical ModelSpec for provider origin"))?;
        let mut total = 0u64;
        for fragment in provider_context {
            let footprint = eviction_footprint_for_payload(&spec, &fragment.payload)
                .context("failed to compute provider-context eviction footprint")?;
            total = total
                .checked_add(footprint.eviction_tokens())
                .ok_or_else(|| anyhow!("eviction footprint overflow"))?;
        }
        Ok(total)
    }

    async fn build_pending_error_context_disposition(
        &self,
        writer: &EventWriter,
    ) -> Result<Option<ErrorContextDisposition>> {
        let Some(pending) = self.pending_error_provider_context.as_ref() else {
            return Ok(None);
        };
        writer
            .build_error_context_disposition(&pending.message_id, pending.message_seq)
            .await
            .map(Some)
    }

    async fn apply_pending_error_context_disposition(
        &mut self,
        writer: &EventWriter,
        disposition: Option<&ErrorContextDisposition>,
    ) -> Result<()> {
        let Some(disposition) = disposition else {
            return Ok(());
        };
        writer.apply_error_context_disposition(disposition).await?;
        let pending = self
            .pending_error_provider_context
            .take()
            .ok_or_else(|| anyhow!("applied Error-context disposition had no pending attempt"))?;
        if disposition.mutation_id().is_empty() {
            bail!(
                "Error-context disposition for {} produced an empty mutation identity",
                pending.message_id
            );
        }
        Ok(())
    }

    pub(super) fn steer_stage(&self) -> SteerStage {
        if self.retry_wait_ready {
            return SteerStage::RetryWait;
        }
        if self.assistant_open.is_some() {
            return SteerStage::AssistantGeneration;
        }
        if !self.pending_tool_end.is_empty()
            || !self.length_not_started.is_empty()
            || !self.pending_tool_calls.is_empty()
            || !self.approval_prepared_tools.is_empty()
            || !self.pending_approval_resolved.is_empty()
            || !self.pending_cancelled_approval_commands.is_empty()
        {
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

        let error_context_disposition =
            self.build_pending_error_context_disposition(writer).await?;
        let mut acks = if let Some(disposition) = error_context_disposition.as_ref() {
            writer
                .apply_active_abort_cutoff_with_error_context_disposition(
                    command.envelope().command_id.as_str(),
                    command.envelope().seq,
                    &self.binding.run_id,
                    disposition.clone(),
                )
                .await?
        } else {
            writer
                .apply_active_abort_cutoff(
                    command.envelope().command_id.as_str(),
                    command.envelope().seq,
                    &self.binding.run_id,
                )
                .await?
        };
        self.apply_pending_error_context_disposition(writer, error_context_disposition.as_ref())
            .await?;

        // Abort supersedes a hard steer after step zero but before the new
        // user message is durably injected.  Drop the staged hand-off only
        // after the cutoff commits; keep `assistant_open` intact so the
        // authoritative interrupted MessageEnd still closes the exact
        // provider message under the original turn identity.
        self.pending_hard_steer = None;
        self.pending_hard_steer_turn_id = None;
        self.pending_hard_steer_inject_batch = None;
        self.pending_hard_steer_user_message_id = None;
        self.pending_hard_steer_partial = None;
        self.pending_hard_steer_message_barrier = None;

        // Abort also supersedes any soft/retry steer group that has been
        // classified but not yet durably injected.  Clear the staged group
        // and all buffered turn-boundary state so the cutoff cannot be
        // followed by a stale group injection on the original owner.
        self.pending_steer_group = None;
        self.pending_steer_turn_end = None;
        self.pending_steer_turn_start = false;
        self.pending_steer_collecting = false;
        self.pending_steer_messages = Vec::new();
        self.pending_steer_open_start = None;

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
        let stage_ok = !self.pending_steer_collecting
            && (matches!(self.steer_stage(), SteerStage::ToolOrApproval)
                || (self.phase == RunPhase::AssistantStarted
                    && self.assistant_open.is_none()
                    && !self.turn_open
                    && self.pending_tool_end.is_empty()));
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

    pub(super) fn take_terminal_command_ids(&mut self) -> Vec<String> {
        std::mem::take(&mut self.committed_terminal_command_ids)
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
        mut output: RunOutput,
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
        if output.approval_command.is_some() {
            self.approval_command = output.approval_command;
        }
        if let Some(tool_call_id) = output.approval_not_started {
            self.approval_not_started.insert(tool_call_id);
        }
        if let Some(tool_call_id) = output.approval_cancelled {
            self.approval_cancelled.insert(tool_call_id);
        }
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
                        // Tool results belong to the closing turn and are committed
                        // before the buffered TurnEnd/TurnStart complete the group.
                        if self.pending_start.is_some() {
                            bail!(
                                "a second non-assistant MessageStart arrived before its MessageEnd"
                            );
                        }
                        self.pending_start = Some((message_id, *message));
                        Ok((Vec::new(), Vec::new()))
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
                    // The group turn is installed atomically by commit_steer_group
                    // after the buffered old TurnEnd closes. Overwriting the durable
                    // binding here would make the buffered TurnEnd reference the new
                    // turn and fail TurnStart/TurnEnd ordering validation.
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
                if let Some(pending) = self.pending_error_provider_context.as_ref() {
                    bail!(
                        "assistant MessageStart is fenced until Error context {} is invalidated",
                        pending.message_id
                    );
                }
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
                // If a soft/retry steer group has already been durably bound,
                // a `Steered` event has already been durably committed, or an
                // abort has been durably bound, any not-yet-started tool is
                // superseded. Drop this start so the worker can emit the
                // skipped ToolResult instead.
                if self.pending_steer_collecting
                    || self.pending_steer_group.is_some()
                    || self.phase == RunPhase::CancelRequested
                {
                    let superseded = self.pending_approval_resolved.iter().any(|(req, _, _)| {
                        self.approval_request_tools.get(req) == Some(&tool_call_id)
                    }) || self.pending_tool_calls.contains(&tool_call_id);
                    if superseded {
                        return Ok(CommittedRunOutput {
                            outputs: Vec::new(),
                            tool_start_barrier: None,
                            message_receipts: Vec::new(),
                            retry_wait_commit_barrier: None,
                            terminal_command_ids: std::mem::take(
                                &mut self.committed_terminal_command_ids,
                            ),
                        });
                    }
                }
                let (grant_status, grant_lease) = output
                    .commit_barrier
                    .as_ref()
                    .expect("ToolExecutionStart barrier checked")
                    .revalidate_grant(
                        &tool_call_id,
                        &tool_name,
                        &args,
                        &self.binding.run_id,
                        &self.binding.turn_id,
                    )
                    .await?;
                if grant_status == GrantRevalidation::Reauthorize {
                    drop(grant_lease);
                    output
                        .commit_barrier
                        .take()
                        .expect("ToolExecutionStart barrier checked")
                        .reauthorize();
                    return Ok(CommittedRunOutput {
                        outputs: Vec::new(),
                        tool_start_barrier: None,
                        message_receipts: Vec::new(),
                        retry_wait_commit_barrier: None,
                        terminal_command_ids: std::mem::take(
                            &mut self.committed_terminal_command_ids,
                        ),
                    });
                }
                self.pending_tool_end
                    .insert(tool_call_id.clone(), (Value::Null, false));
                self.pending_tool_calls.remove(&tool_call_id);
                if let Some(pos) = self
                    .pending_approval_resolved
                    .iter()
                    .position(|(req, _, _)| {
                        self.approval_request_tools.get(req) == Some(&tool_call_id)
                    })
                {
                    let (request_id, resolution, command) =
                        self.pending_approval_resolved.remove(pos);
                    let expected_tool_call_id = self
                        .approval_request_tools
                        .get(&request_id)
                        .ok_or_else(|| anyhow!("pending approval has no tool_call_id binding"))?;
                    if *expected_tool_call_id != tool_call_id {
                        bail!("ToolExecutionStart does not match the pending approval resolution");
                    }
                    let state = approval_state(&resolution);
                    let command_id = command.envelope().command_id.to_string();
                    let command_seq = command.envelope().seq;
                    let run_id = self.binding.run_id.clone();
                    let public_resolution = resolution.clone();
                    let mut resolution_projections = vec![
                        Projection::Approval(ApprovalMutation::Resolve {
                            request_id: request_id.clone(),
                            state,
                            actor: "user".to_owned(),
                        }),
                        Projection::CommandApplied {
                            command_id: command_id.clone(),
                            command_seq,
                            run_id: Some(run_id.clone()),
                        },
                    ];
                    if let Some(rule) = approval_rule_projection(&public_resolution)? {
                        resolution_projections.push(Projection::ApprovalRule(rule));
                    }
                    let writes = vec![
                        EventWrite {
                            event: Some(DurableEvent::approval_resolved(
                                request_id.clone(),
                                resolution,
                                "user".to_owned(),
                            )?),
                            projections: resolution_projections,
                        },
                        EventWrite {
                            event: Some(DurableEvent::tool_execution_start(
                                tool_call_id.clone(),
                                tool_name.clone(),
                                args.clone(),
                                self.binding.command_id.clone(),
                                run_id.clone(),
                                self.binding.executor_generation,
                            )?),
                            projections: vec![Projection::ToolExecution(
                                ToolExecutionMutation::Start {
                                    tool_call_id: tool_call_id.clone(),
                                    run_id: run_id.clone(),
                                },
                            )],
                        },
                    ];
                    self.approval_prepared_tools.remove(&tool_call_id);
                    self.committed_terminal_command_ids.push(command_id);
                    let public = vec![
                        AgentEvent::ApprovalResolved {
                            request_id: request_id.clone(),
                            resolution: public_resolution,
                        },
                        AgentEvent::ToolExecutionStart {
                            tool_call_id: tool_call_id.clone(),
                            tool_name: tool_name.clone(),
                            args: args.clone(),
                        },
                    ];
                    let outputs = self
                        .commit_batch(
                            writer,
                            EventBatch {
                                writes,
                                injected_commands: Vec::new(),
                            },
                            public,
                        )
                        .await?;
                    drop(grant_lease);
                    return Ok(CommittedRunOutput {
                        outputs,
                        tool_start_barrier: output.commit_barrier,
                        message_receipts: Vec::new(),
                        retry_wait_commit_barrier: None,
                        terminal_command_ids: std::mem::take(
                            &mut self.committed_terminal_command_ids,
                        ),
                    });
                }
                let mut projections = Vec::with_capacity(2);
                if !self.approval_prepared_tools.remove(&tool_call_id) {
                    projections.push(Projection::ToolExecution(ToolExecutionMutation::Prepare {
                        tool_call_id: tool_call_id.clone(),
                        command_id: self.binding.command_id.clone(),
                        run_id: self.binding.run_id.clone(),
                        executor_generation: self.binding.executor_generation,
                        idempotency_key: self.binding.tool_execution_idempotency_key(&tool_call_id),
                    }));
                }
                projections.push(Projection::ToolExecution(ToolExecutionMutation::Start {
                    tool_call_id: tool_call_id.clone(),
                    run_id: self.binding.run_id.clone(),
                }));
                let committed = self
                    .commit_single(
                        writer,
                        DurableEvent::tool_execution_start(
                            tool_call_id.clone(),
                            tool_name.clone(),
                            args.clone(),
                            self.binding.command_id.clone(),
                            self.binding.run_id.clone(),
                            self.binding.executor_generation,
                        )?,
                        projections,
                        AgentEvent::ToolExecutionStart {
                            tool_call_id,
                            tool_name,
                            args,
                        },
                    )
                    .await?;
                drop(grant_lease);
                Ok(committed)
            }
            AgentEvent::RetryScheduled {
                attempt,
                delay_ms,
                retry_at,
                error_message,
            } => {
                let error_context_disposition =
                    self.build_pending_error_context_disposition(writer).await?;
                let projections = error_context_disposition
                    .iter()
                    .cloned()
                    .map(Projection::ProviderContextMutationPrepare)
                    .collect();
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
                        projections,
                        AgentEvent::RetryScheduled {
                            attempt,
                            delay_ms,
                            retry_at,
                            error_message,
                        },
                    )
                    .await?;
                self.apply_pending_error_context_disposition(
                    writer,
                    error_context_disposition.as_ref(),
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
                if let Some(group) = self.pending_steer_group.as_ref()
                    && group.application_kind() == ApplicationKind::SoftSteer
                    && self.pending_steer_turn_end.is_none()
                {
                    if self.pending_error_provider_context.is_some() {
                        bail!(
                            "terminal Error TurnEnd cannot be buffered past its context disposition"
                        );
                    }
                    self.pending_steer_turn_end = Some(((*message).clone(), tool_results.clone()));
                    Ok((Vec::new(), Vec::new()))
                } else {
                    let error_context_disposition =
                        self.build_pending_error_context_disposition(writer).await?;
                    let projections = error_context_disposition
                        .iter()
                        .cloned()
                        .map(Projection::ProviderContextMutationPrepare)
                        .collect();
                    self.turn_open = false;
                    let committed = self
                        .commit_single(
                            writer,
                            DurableEvent::turn_end(
                                &self.binding.run_id,
                                &self.binding.turn_id,
                                (*message).clone(),
                                tool_results.clone(),
                            )?,
                            projections,
                            AgentEvent::TurnEnd {
                                message: Some(message),
                                tool_results,
                            },
                        )
                        .await?;
                    self.apply_pending_error_context_disposition(
                        writer,
                        error_context_disposition.as_ref(),
                    )
                    .await?;
                    Ok(committed)
                }
            }
            AgentEvent::AgentEnd => {
                if let Some(pending) = self.pending_error_provider_context.as_ref() {
                    bail!(
                        "AgentEnd is fenced until Error context {} is invalidated",
                        pending.message_id
                    );
                }
                if self.turn_open {
                    bail!("AgentEnd requires the current TurnEnd to be durable first");
                }
                if !self.pending_approval_resolved.is_empty() {
                    bail!("AgentEnd cannot commit while approved tools have not started");
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
                let (agent_end_outputs, message_receipts) = self
                    .commit_single(
                        writer,
                        DurableEvent::agent_end(&self.binding.run_id)?,
                        projections,
                        AgentEvent::AgentEnd,
                    )
                    .await?;
                Ok((agent_end_outputs, message_receipts))
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
                if !self.pending_tool_calls.remove(&tool_call_id) {
                    bail!("ApprovalRequested does not match an unprepared tool call");
                }
                self.approval_request_tools
                    .insert(request_id.clone(), tool_call_id.clone());
                self.approval_prepared_tools.insert(tool_call_id.clone());
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
                if let Some(pos) = self
                    .pending_approval_resolved
                    .iter()
                    .position(|(rid, _, _)| rid == &request_id)
                {
                    let (request_id, _, command) = self.pending_approval_resolved.remove(pos);
                    let tool_call_id = self
                        .approval_request_tools
                        .get(&request_id)
                        .cloned()
                        .ok_or_else(|| anyhow!("cancelled approval has no tool binding"))?;
                    if self
                        .pending_cancelled_approval_commands
                        .insert(tool_call_id, (request_id, command))
                        .is_some()
                    {
                        bail!("duplicate pre-start cancelled approval command");
                    }
                    return Ok(CommittedRunOutput {
                        outputs: Vec::new(),
                        tool_start_barrier: None,
                        message_receipts: Vec::new(),
                        retry_wait_commit_barrier: None,
                        terminal_command_ids: std::mem::take(
                            &mut self.committed_terminal_command_ids,
                        ),
                    });
                } else if let Some(tool_call_id) = self.approval_request_tools.get(&request_id) {
                    self.approval_cancelled.insert(tool_call_id.clone());
                    self.approval_prepared_tools.remove(tool_call_id);
                }
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
            AgentEvent::ApprovalResolved {
                request_id,
                resolution: resolution @ ApprovalResolution::Decision(..),
            } => {
                if self.phase != RunPhase::AssistantStarted
                    || !self.turn_open
                    || self.assistant_open.is_some()
                {
                    bail!(
                        "ApprovalResolved Decision requires a durable assistant tool call in the active turn"
                    );
                }
                let state = approval_state(&resolution);
                if matches!(
                    resolution,
                    ApprovalResolution::Decision(ApprovalDecision::Deny)
                ) && let Some(tool_call_id) = self.approval_request_tools.get(&request_id)
                {
                    self.approval_not_started.insert(tool_call_id.clone());
                }
                let command = self.approval_command.take().ok_or_else(|| {
                    anyhow!("ApprovalResolved Decision requires a pending command")
                })?;
                if matches!(
                    resolution,
                    ApprovalResolution::Decision(
                        ApprovalDecision::ApproveOnce | ApprovalDecision::ApproveAlways { .. }
                    )
                ) {
                    self.pending_approval_resolved
                        .push((request_id, resolution, command));
                    return Ok(CommittedRunOutput {
                        outputs: Vec::new(),
                        tool_start_barrier: None,
                        message_receipts: Vec::new(),
                        retry_wait_commit_barrier: None,
                        terminal_command_ids: std::mem::take(
                            &mut self.committed_terminal_command_ids,
                        ),
                    });
                } else {
                    let command_id = command.envelope().command_id.to_string();
                    let command_seq = command.envelope().seq;
                    let run_id = self.binding.run_id.clone();
                    let projections = vec![
                        Projection::Approval(ApprovalMutation::Resolve {
                            request_id: request_id.clone(),
                            state,
                            actor: "user".to_owned(),
                        }),
                        Projection::CommandApplied {
                            command_id: command_id.clone(),
                            command_seq,
                            run_id: Some(run_id),
                        },
                    ];
                    self.committed_terminal_command_ids.push(command_id);
                    self.commit_single(
                        writer,
                        DurableEvent::approval_resolved(
                            request_id.clone(),
                            resolution.clone(),
                            "user".to_owned(),
                        )?,
                        projections,
                        AgentEvent::ApprovalResolved {
                            request_id,
                            resolution,
                        },
                    )
                    .await
                }
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
        let provider_context = barrier.provider_context().to_vec();
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
        let mut projections = vec![Projection::MessageEnd {
            message_id: message_id.clone(),
            role: "assistant",
            message: message.clone(),
            append_to_l0,
            eviction_footprint_tokens: Self::provider_context_footprint(
                &message,
                &provider_context,
            )?,
            provider_context,
        }];
        if let Some(uncalibrated_prompt_estimate) = barrier.calibration_estimate() {
            projections.push(Projection::MemoryCalibrationObservation {
                uncalibrated_prompt_estimate,
            });
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
                    projections,
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
            provider_context: Vec::new(),
            eviction_footprint_tokens: 0,
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
            } else if self.approval_not_started.contains(&tool_call_id)
                && self.pending_tool_calls.contains(&tool_call_id)
            {
                if !result.is_error {
                    bail!("Approval-not-started ToolResult must be is_error=true");
                }
                self.approval_not_started.remove(&tool_call_id);
                self.pending_tool_calls.remove(&tool_call_id);
                projections.push(Projection::ToolExecution(ToolExecutionMutation::Skip {
                    tool_call_id: tool_call_id.clone(),
                    command_id: self.binding.command_id.clone(),
                    run_id: self.binding.run_id.clone(),
                    turn_id: self.binding.turn_id.clone(),
                    executor_generation: self.binding.executor_generation,
                    idempotency_key: self.binding.tool_execution_idempotency_key(&tool_call_id),
                    error_code: "approval_denied",
                }));
            } else if self.pending_tool_calls.remove(&tool_call_id) {
                if !result.is_error {
                    bail!("Cancelled ToolResult must be is_error=true");
                }
                // After an Abort the binding has advanced to the abort control;
                // the cancelled tool call still belongs to the original owner.
                let owner_id = self
                    .aborted_owner
                    .as_ref()
                    .map(|(id, _)| id.clone())
                    .unwrap_or_else(|| self.binding.command_id.clone());
                projections.push(Projection::ToolExecution(ToolExecutionMutation::Skip {
                    tool_call_id: tool_call_id.clone(),
                    command_id: owner_id.clone(),
                    run_id: self.binding.run_id.clone(),
                    turn_id: self.binding.turn_id.clone(),
                    executor_generation: self.binding.executor_generation,
                    idempotency_key: format!("{owner_id}/{tool_call_id}"),
                    error_code: "user_steer_cancelled",
                }));
            } else if self.length_not_started.remove(&tool_call_id) {
                if !result.is_error {
                    bail!("Length-not-started ToolResult must be is_error=true");
                }
                // A Length-guarded assistant that is then aborted or hard-steered
                // leaves the original owner in cancel_requested/hard_steer_requested.
                // The canonical skip for those not-started calls is user_steer_cancelled;
                // length_guard is reserved to the assistant's own turn.
                let owner_id = self
                    .aborted_owner
                    .as_ref()
                    .map(|(id, _)| id.clone())
                    .unwrap_or_else(|| self.binding.command_id.clone());
                let steer_cancelled =
                    self.aborted_owner.is_some() || self.pending_hard_steer.is_some();
                let error_code = if steer_cancelled {
                    "user_steer_cancelled"
                } else {
                    "length_guard"
                };
                projections.push(Projection::ToolExecution(ToolExecutionMutation::Skip {
                    tool_call_id: tool_call_id.clone(),
                    command_id: owner_id.clone(),
                    run_id: self.binding.run_id.clone(),
                    turn_id: self.binding.turn_id.clone(),
                    executor_generation: self.binding.executor_generation,
                    idempotency_key: format!("{owner_id}/{tool_call_id}"),
                    error_code,
                }));
            } else if self.approval_cancelled.remove(&tool_call_id) {
                if !result.is_error {
                    bail!("Approval-cancelled ToolResult must be is_error=true");
                }
                self.approval_prepared_tools.remove(&tool_call_id);
                let result_value = serde_json::to_value(result)?;
                if let Some((request_id, command)) = self
                    .pending_cancelled_approval_commands
                    .remove(&tool_call_id)
                {
                    let command_id = command.envelope().command_id.to_string();
                    writes.push(EventWrite {
                        event: Some(DurableEvent::approval_resolved(
                            request_id.clone(),
                            ApprovalResolution::Cancelled,
                            "runtime".to_owned(),
                        )?),
                        projections: vec![
                            Projection::Approval(ApprovalMutation::Resolve {
                                request_id: request_id.clone(),
                                state: "cancelled",
                                actor: "runtime".to_owned(),
                            }),
                            Projection::CommandApplied {
                                command_id: command_id.clone(),
                                command_seq: command.envelope().seq,
                                run_id: None,
                            },
                        ],
                    });
                    self.committed_terminal_command_ids.push(command_id);
                    public_prefix.push(AgentEvent::ApprovalResolved {
                        request_id,
                        resolution: ApprovalResolution::Cancelled,
                    });
                }
                writes.push(EventWrite {
                    event: Some(DurableEvent::tool_execution_end(
                        tool_call_id.clone(),
                        result_value.clone(),
                        true,
                        "cancelled".to_owned(),
                        Some("approval_cancelled".to_owned()),
                    )?),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                        tool_call_id: tool_call_id.clone(),
                        expected: "prepared",
                        state: "cancelled",
                        error_code: Some("approval_cancelled"),
                    })],
                });
                public_prefix.push(AgentEvent::ToolExecutionEnd {
                    tool_call_id: tool_call_id.clone(),
                    result: result_value,
                    is_error: true,
                });
            } else if self.approval_not_started.remove(&tool_call_id) {
                if !result.is_error {
                    bail!("Approval-not-started ToolResult must be is_error=true");
                }
                self.approval_prepared_tools.remove(&tool_call_id);
                let result_value = serde_json::to_value(result)?;
                writes.push(EventWrite {
                    event: Some(DurableEvent::tool_execution_end(
                        tool_call_id.clone(),
                        result_value.clone(),
                        true,
                        "cancelled".to_owned(),
                        Some("approval_denied".to_owned()),
                    )?),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                        tool_call_id: tool_call_id.clone(),
                        expected: "prepared",
                        state: "cancelled",
                        error_code: Some("approval_denied"),
                    })],
                });
                public_prefix.push(AgentEvent::ToolExecutionEnd {
                    tool_call_id: tool_call_id.clone(),
                    result: result_value,
                    is_error: true,
                });
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
                    "tool result has neither execution lifecycle nor known not-started disposition"
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
        let provider_context = assistant_barrier.provider_context().to_vec();
        let mut assistant_projections = vec![Projection::MessageEnd {
            message_id: assistant_id.clone(),
            role: "assistant",
            message: assistant.clone(),
            append_to_l0,
            eviction_footprint_tokens: Self::provider_context_footprint(
                &assistant,
                &provider_context,
            )?,
            provider_context,
        }];
        if let Some(uncalibrated_prompt_estimate) = assistant_barrier.calibration_estimate() {
            assistant_projections.push(Projection::MemoryCalibrationObservation {
                uncalibrated_prompt_estimate,
            });
        }
        let mut writes = vec![EventWrite {
            event: Some(DurableEvent::message_in_turn(
                "message_end",
                &assistant_id,
                &assistant,
                Some(self.binding.run_id.clone()),
                Some(self.binding.turn_id.clone()),
            )?),
            projections: assistant_projections,
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
                    provider_context: Vec::new(),
                    eviction_footprint_tokens: 0,
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
        let error_context_message_ids: Vec<String> = batch
            .writes
            .iter()
            .flat_map(|write| &write.projections)
            .filter_map(|projection| match projection {
                Projection::MessageEnd {
                    message_id,
                    message: PublicMessage::Assistant(assistant),
                    append_to_l0: false,
                    provider_context,
                    ..
                } if assistant.stop_reason == StopReason::Error && !provider_context.is_empty() => {
                    Some(message_id.clone())
                }
                _ => None,
            })
            .collect();
        if error_context_message_ids.len() > 1 {
            bail!("one EventBatch cannot open multiple Error-context retention units");
        }
        if self.pending_error_provider_context.is_some() && !error_context_message_ids.is_empty() {
            bail!("a prior Error-context retention unit has not reached applied");
        }

        self.collect_terminal_command_ids(&batch);
        let (seqs, calibration_ratio_bits) = writer.apply_with_calibration_receipt(batch).await?;
        if seqs.len() != public.len() {
            bail!("durable EventBatch/public event cardinality mismatch");
        }
        let outputs: Vec<_> = public
            .into_iter()
            .zip(seqs)
            .map(|(event, seq)| CommittedOutput {
                event,
                seq: Some(seq),
            })
            .collect();
        let calibration_receipt_count = receipt_requests
            .iter()
            .filter(|(_, barrier)| barrier.calibration_estimate().is_some())
            .count();
        match (calibration_receipt_count, calibration_ratio_bits) {
            (0, None) | (1, Some(_)) => {}
            (0, Some(_)) => {
                bail!("calibration projection has no receipt-bound MessageEnd");
            }
            (_, None) => {
                bail!("calibration receipt-bound MessageEnd committed no calibration");
            }
            (_, Some(_)) => {
                bail!("multiple MessageEnd receipts requested one calibration value");
            }
        }
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
        if let Some(message_id) = error_context_message_ids.into_iter().next() {
            let message_seq = *committed_by_id.get(&message_id).ok_or_else(|| {
                anyhow!("Error-context MessageEnd {message_id} has no committed sequence")
            })?;
            self.pending_error_provider_context = Some(PendingErrorProviderContext {
                message_id,
                message_seq,
            });
        }
        let mut receipts = Vec::with_capacity(receipt_requests.len());
        for (message_id, barrier) in receipt_requests {
            let message_seq = committed_by_id.remove(&message_id).ok_or_else(|| {
                anyhow!("atomic batch omitted receipt-bound MessageEnd {message_id}")
            })?;
            let receipt_calibration_ratio_bits = barrier
                .calibration_estimate()
                .map(|_| calibration_ratio_bits.expect("calibration receipt was checked"));
            receipts.push((
                barrier,
                MessageCommitReceipt {
                    message_id,
                    message_seq,
                    calibration_ratio_bits: receipt_calibration_ratio_bits,
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
        let provider_context = barrier.provider_context().to_vec();
        let eviction_footprint_tokens =
            Self::provider_context_footprint(&normalized, &provider_context)?;
        let new_turn_id = self
            .pending_hard_steer_turn_id
            .take()
            .ok_or_else(|| anyhow!("hard-steer partial MessageEnd has no pending turn id"))?;
        let mut batches = finalize_hard_steer_batches(
            &self.binding,
            &command,
            message_id.clone(),
            message,
            provider_context,
            eviction_footprint_tokens,
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
            calibration_ratio_bits: None,
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
        let (start_id, start_message) = self
            .pending_start
            .take()
            .ok_or_else(|| anyhow!("hard-steer user MessageEnd has no buffered MessageStart"))?;
        if start_id != message_id || start_message != message {
            bail!("hard-steer user MessageStart/End pair is not exact");
        }
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
                    calibration_ratio_bits: None,
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
        let mut next_seq = || {
            seq_iter
                .next()
                .ok_or_else(|| anyhow!("steer group committed fewer durable seqs than expected"))
        };
        if is_soft {
            let turn_end_seq = next_seq()?;
            public.push(CommittedOutput {
                event: AgentEvent::TurnEnd {
                    message: closing_turn_message.map(Box::new),
                    tool_results: closing_tool_results.clone(),
                },
                seq: Some(turn_end_seq),
            });
        }
        for _ in 0..group_len {
            let seq = next_seq()?;
            public.push(CommittedOutput {
                event: AgentEvent::Steered {
                    mode: super::SteerMode::Soft,
                },
                seq: Some(seq),
            });
        }
        if is_soft {
            let turn_start_seq = next_seq()?;
            public.push(CommittedOutput {
                event: AgentEvent::TurnStart,
                seq: Some(turn_start_seq),
            });
        }
        let message_start_base = public.len();
        for (index, command) in commands.iter().enumerate() {
            let user_message = super::steer::build_user_message(command)?;
            let user_message_id = crate::store::user_message_id(&command.envelope().command_id);
            let start_seq = next_seq()?;
            let end_seq = next_seq()?;
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
                    calibration_ratio_bits: None,
                    new_turn_id: None,
                },
            ));
        }

        assert_eq!(
            public.len(),
            expected_writes,
            "steer group public event count mismatch"
        );

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

fn approval_state(resolution: &ApprovalResolution) -> &'static str {
    use crate::gateway::ApprovalDecision;
    match resolution {
        ApprovalResolution::Decision(ApprovalDecision::ApproveOnce) => "approved_once",
        ApprovalResolution::Decision(ApprovalDecision::ApproveAlways { .. }) => "approved_always",
        ApprovalResolution::Decision(ApprovalDecision::Deny) => "denied",
        ApprovalResolution::Cancelled => "cancelled",
    }
}

fn approval_rule_projection(
    resolution: &ApprovalResolution,
) -> Result<Option<ApprovalRuleMutation>> {
    let ApprovalResolution::Decision(ApprovalDecision::ApproveAlways { rule }) = resolution else {
        return Ok(None);
    };
    let value = serde_json::to_value(rule)?;
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ApproveAlways rule has no id"))?
        .to_owned();
    let tool = value
        .get("tool")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ApproveAlways rule has no tool"))?
        .to_owned();
    Ok(Some(ApprovalRuleMutation {
        id,
        tool,
        pattern: serde_json::to_string(&value)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        agent::{
            AdmittedCommand,
            events::{ApprovalRequest, ApprovalResolution, ReviewProjection},
        },
        gateway::{ApprovalDecision, Command, CommandEnvelope, CommandId, InboundCommand},
        memory::estimate::{ProviderContextItemWithFootprint, eviction_footprint_for_payload},
        provider::{
            ModelSpec,
            types::{
                ApiProtocol, ContextMessage, Message, NativeCompactionCoverage,
                ProviderContextAnchor, ProviderContextFragment, ProviderContextItem,
                ProviderContextPayload, ProviderOrigin, PublicAssistantContent,
                PublicAssistantMessage, PublicMessage, StopReason, ToolCall, ToolResultMessage,
                UserContent, UserMessage, ValidatedToolArguments,
            },
        },
        store::{
            ApplicationKind, DurableEvent, EventBatch, EventWriter, InjectedCommand, Projection,
            RunPhase, Store,
        },
    };

    fn test_timestamp() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2026-07-20T01:02:03.456789Z")
            .expect("valid test timestamp")
            .with_timezone(&chrono::Utc)
    }

    fn test_user_command(seq: u64, command_id: &str, text: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            seq,
            command_id: CommandId::parse(command_id).expect("canonical test UUID"),
            command: Command::UserMessage {
                text: text.to_owned(),
                attachments: Vec::new(),
            },
        })
    }

    fn test_admitted(seq: u64, command_id: &str, text: &str) -> AdmittedCommand {
        AdmittedCommand::new(
            CommandEnvelope {
                seq,
                command_id: CommandId::parse(command_id).expect("canonical test UUID"),
                command: Command::UserMessage {
                    text: text.to_owned(),
                    attachments: Vec::new(),
                },
            },
            test_timestamp(),
        )
    }

    fn test_abort_command(seq: u64, command_id: &str) -> InboundCommand {
        InboundCommand::Valid(CommandEnvelope {
            seq,
            command_id: CommandId::parse(command_id).expect("canonical test UUID"),
            command: Command::Abort {},
        })
    }

    fn test_admitted_abort(seq: u64, command_id: &str) -> AdmittedCommand {
        AdmittedCommand::new(
            CommandEnvelope {
                seq,
                command_id: CommandId::parse(command_id).expect("canonical test UUID"),
                command: Command::Abort {},
            },
            test_timestamp(),
        )
    }

    fn test_admitted_approval_decision(
        seq: u64,
        command_id: &str,
        request_id: &str,
        decision: ApprovalDecision,
    ) -> AdmittedCommand {
        AdmittedCommand::new(
            CommandEnvelope {
                seq,
                command_id: CommandId::parse(command_id).expect("canonical test UUID"),
                command: Command::ApprovalDecision {
                    request_id: request_id.to_owned(),
                    decision,
                },
            },
            test_timestamp(),
        )
    }

    async fn test_store() -> std::sync::Arc<Store> {
        Store::session_test_store("durable-bridge-test")
            .await
            .expect("open test store")
            .into()
    }

    async fn persist_and_pin(
        store: &Store,
        writer: &EventWriter,
        seq: u64,
        command_id: &str,
        text: &str,
    ) -> chrono::DateTime<chrono::Utc> {
        let timestamp = test_timestamp();
        writer
            .persist_inbound(&test_user_command(seq, command_id, text))
            .await
            .expect("persist command");
        sqlx::query("UPDATE inbound_commands SET received_at=? WHERE command_id=?")
            .bind(timestamp.to_rfc3339())
            .bind(command_id)
            .execute(store.pool())
            .await
            .expect("pin durable timestamp");
        timestamp
    }

    async fn owner_in_phase(
        store: &Store,
        writer: &EventWriter,
        command_id: &str,
        run_id: &str,
        turn_id: &str,
        phase: RunPhase,
    ) -> (DurableRunBinding, Option<(String, PublicMessage)>) {
        owner_in_phase_with_origin(
            store,
            writer,
            command_id,
            run_id,
            turn_id,
            phase,
            ProviderOrigin {
                provider_instance_id: "test".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "test".to_owned(),
            },
        )
        .await
    }

    async fn owner_in_phase_with_origin(
        store: &Store,
        writer: &EventWriter,
        command_id: &str,
        run_id: &str,
        turn_id: &str,
        phase: RunPhase,
        assistant_origin: ProviderOrigin,
    ) -> (DurableRunBinding, Option<(String, PublicMessage)>) {
        let binding = DurableRunBinding {
            command_id: command_id.to_owned(),
            command_seq: 1,
            run_id: run_id.to_owned(),
            turn_id: turn_id.to_owned(),
            executor_generation: crate::runtime::contracts::ProcessGeneration::MIN,
        };
        let assistant_message_id = format!("{}-assistant", command_id);

        let _ = persist_and_pin(store, writer, 1, command_id, "owner").await;

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: command_id.to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: run_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify owner");

        let message_id = crate::store::user_message_id(command_id);
        let message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "owner".to_owned(),
            }],
            timestamp: test_timestamp(),
        });

        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(DurableEvent::agent_start(run_id).expect("AgentStart")),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.to_owned(),
                            run_id: run_id.to_owned(),
                            expected: RunPhase::Classified,
                            next: RunPhase::RunStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(DurableEvent::turn_start(run_id, turn_id).expect("TurnStart")),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.to_owned(),
                            run_id: run_id.to_owned(),
                            expected: RunPhase::RunStarted,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", &message_id, &message)
                                .expect("MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.to_owned(),
                            run_id: run_id.to_owned(),
                            expected: RunPhase::TurnStarted,
                            next: RunPhase::UserStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", &message_id, &message)
                                .expect("MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: message_id.clone(),
                                role: "user",
                                message,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            Projection::RunPhase {
                                command_id: command_id.to_owned(),
                                run_id: run_id.to_owned(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(
                    1,
                    CommandId::parse(command_id).expect("canonical"),
                )],
            })
            .await
            .expect("inject owner");

        let mut assistant_message: Option<(String, PublicMessage)> = None;
        if phase == RunPhase::AssistantStarted {
            let assistant = PublicMessage::Assistant(PublicAssistantMessage {
                content: Vec::new(),
                model: assistant_origin.model.clone(),
                provider: "test".to_owned(),
                origin: assistant_origin,
                usage: crate::provider::types::Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: test_timestamp(),
            });
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                &assistant_message_id,
                                &assistant,
                                Some(run_id.to_owned()),
                                Some(turn_id.to_owned()),
                            )
                            .expect("assistant MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: command_id.to_owned(),
                            run_id: run_id.to_owned(),
                            expected: RunPhase::UserCommitted,
                            next: RunPhase::AssistantStarted,
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("transition owner to assistant_started");
            assistant_message = Some((assistant_message_id.clone(), assistant.clone()));
        } else if phase != RunPhase::UserCommitted {
            panic!("owner_in_phase does not support {}", phase.as_str());
        }

        (binding, assistant_message)
    }

    struct ErrorContextFixture {
        store: std::sync::Arc<Store>,
        writer: EventWriter,
        bridge: DurableBridge,
        binding: DurableRunBinding,
        message_id: String,
        message: PublicMessage,
        item_key_ref: String,
    }

    #[derive(Clone, Copy, Debug)]
    enum ErrorContextShape {
        NativeOnly,
        Mixed,
    }

    impl ErrorContextShape {
        fn item_count(self) -> i64 {
            match self {
                Self::NativeOnly => 1,
                Self::Mixed => 2,
            }
        }
    }

    async fn error_context_fixture(
        owner_id: &str,
        run_id: &str,
        turn_id: &str,
        shape: ErrorContextShape,
    ) -> ErrorContextFixture {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        let (binding, owner_assistant) = owner_in_phase_with_origin(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
            spec.origin(),
        )
        .await;
        let (message_id, _) = owner_assistant.expect("owner assistant");
        let message = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
                text: "verified partial".to_owned(),
                wire_item_index: 0,
            }],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: spec.origin(),
            usage: crate::provider::types::Usage {
                input: 17,
                output: 3,
                total_tokens: 20,
                ..crate::provider::types::Usage::default()
            },
            stop_reason: StopReason::Error,
            error_message: Some("provider display error".to_owned()),
            provider_code: Some("network_error".to_owned()),
            interrupted: false,
            timestamp: test_timestamp(),
        });
        let coverage_seq = u64::try_from(
            sqlx::query_scalar::<_, i64>("SELECT MAX(seq) FROM messages")
                .fetch_one(store.pool())
                .await
                .expect("load native Error coverage endpoint"),
        )
        .expect("coverage endpoint is positive");
        let mut fragments = vec![ProviderContextFragment {
            wire_item_index: None,
            payload: ProviderContextPayload::OpenAiCompactedWindow {
                items: vec![serde_json::json!({
                    "id": format!("cmp-{owner_id}"),
                    "type": "compaction",
                    "encrypted_content": "opaque-error-native",
                })],
                coverage: NativeCompactionCoverage {
                    through_message_seq: coverage_seq,
                    context_fingerprint: format!("error-native-{owner_id}"),
                },
            },
        }];
        if matches!(shape, ErrorContextShape::Mixed) {
            fragments.push(ProviderContextFragment {
                wire_item_index: Some(1),
                payload: ProviderContextPayload::EncryptedReasoning {
                    protocol: ApiProtocol::OpenAiResponses,
                    item: serde_json::json!({
                        "id": format!("rs-{owner_id}"),
                        "type": "reasoning",
                        "summary": [],
                        "encrypted_content": "opaque-error-reasoning",
                    }),
                },
            });
        }

        let mut bridge = DurableBridge::new(binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(message_id.clone());
        let (barrier, receipt) = MessageCommitBarrier::channel_with_provider_context(fragments);
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: message_id.clone(),
                        message: Box::new(message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit authoritative Error terminal");
        committed.resolve_message_receipts();
        let committed_receipt = receipt.await.expect("Error MessageEnd receipt");
        assert_eq!(committed_receipt.message_id, message_id);
        assert_eq!(
            bridge.pending_error_provider_context,
            Some(PendingErrorProviderContext {
                message_id: message_id.clone(),
                message_seq: committed_receipt.message_seq,
            })
        );
        let (item_key_ref, item_count): (String, i64) =
            sqlx::query_as("SELECT MIN(key_ref), COUNT(*) FROM provider_context")
                .fetch_one(store.pool())
                .await
                .expect("load mixed Error provider-context retention unit");
        assert_eq!(
            item_count,
            shape.item_count(),
            "fixture must retain its exact Error context shape"
        );
        ErrorContextFixture {
            store,
            writer,
            bridge,
            binding,
            message_id,
            message,
            item_key_ref,
        }
    }

    async fn assert_error_context_applied(fixture: &ErrorContextFixture) {
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM provider_context")
                .fetch_one(fixture.store.pool())
                .await
                .expect("count Error provider-context rows"),
            0
        );
        let (state, wrapped_key): (String, Option<Vec<u8>>) =
            sqlx::query_as("SELECT state, wrapped_key FROM data_keys WHERE key_ref = ?")
                .bind(&fixture.item_key_ref)
                .fetch_one(fixture.store.pool())
                .await
                .expect("read Error provider-context key");
        assert_eq!(state, "destroyed");
        assert!(wrapped_key.is_none());
        assert!(fixture.bridge.pending_error_provider_context.is_none());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM provider_context_mutations
                 WHERE state = 'applied'",
            )
            .fetch_one(fixture.store.pool())
            .await
            .expect("count applied Error dispositions"),
            1
        );
    }

    fn binding(command_id: &str) -> DurableRunBinding {
        DurableRunBinding {
            command_id: command_id.to_owned(),
            command_seq: 1,
            run_id: "run-a".to_owned(),
            turn_id: "turn-a".to_owned(),
            executor_generation: ProcessGeneration::from_wire(73).unwrap(),
        }
    }

    #[tokio::test]
    async fn assistant_message_receipt_carries_exact_committed_calibration_bits() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let command_id = "00000000-0000-4000-8000-000000000031";
        let (binding, assistant) = owner_in_phase(
            &store,
            &writer,
            command_id,
            "run-calibration",
            "turn-calibration",
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, mut assistant) = assistant.expect("assistant-started fixture");
        let PublicMessage::Assistant(message) = &mut assistant else {
            panic!("assistant-started fixture returned a non-assistant message");
        };
        message.usage.input = 200;
        message.usage.total_tokens = 200;

        let mut bridge = DurableBridge::new(binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());
        let (barrier, receiver) =
            MessageCommitBarrier::channel_with_provider_context_and_calibration(Vec::new(), 100);
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding,
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id.clone(),
                        message: Box::new(assistant),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit calibrated assistant MessageEnd");
        committed.resolve_message_receipts();
        let receipt = receiver.await.expect("calibration receipt");

        assert_eq!(receipt.message_id, assistant_id);
        let receipt_bits = receipt
            .calibration_ratio_bits
            .expect("calibrated MessageEnd receipt carries ratio bits");
        let stored_bits: Vec<u8> =
            sqlx::query_scalar("SELECT ratio_bits FROM memory_calibration WHERE singleton = 1")
                .fetch_one(store.pool())
                .await
                .expect("committed calibration row");
        assert_eq!(stored_bits, receipt_bits);
        assert!(
            (f64::from_bits(u64::from_be_bytes(receipt_bits)) - 1.3).abs() < 1.0e-12,
            "default 1.0 ratio updated from a 200/100 observation with alpha 0.3"
        );
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
            approval_command: None,
            approval_not_started: None,
            approval_cancelled: None,
        };
        assert_eq!(output.binding.executor_generation.to_wire(), 73);
        let public = serde_json::to_value(output.event).expect("serialize public event");
        assert_eq!(public, serde_json::json!({"type":"agent_start"}));
        assert!(public.get("executor_generation").is_none());
    }

    #[tokio::test]
    async fn can_bind_soft_steer_rejects_while_collecting_group_messages() {
        use crate::gateway::{CommandEnvelope, CommandId};
        use chrono::Utc;

        let store = std::sync::Arc::new(
            crate::store::Store::session_test_store("test-soft-steer-collecting")
                .await
                .expect("open test store"),
        );
        let writer = EventWriter::new(store);
        let mut bridge = DurableBridge::new(binding("00000000-0000-4000-8000-000000000001"));
        bridge.phase = RunPhase::AssistantStarted;
        bridge.pending_steer_collecting = true;

        let command = AdmittedCommand::new(
            CommandEnvelope {
                seq: 2,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000002")
                    .expect("canonical test command id"),
                command: Command::UserMessage {
                    text: "interleaved user".to_owned(),
                    attachments: Vec::new(),
                },
            },
            Utc::now(),
        );

        assert!(
            !bridge.can_bind_soft_steer(&writer, &command),
            "soft steer must not bind while a steer group is collecting messages"
        );
    }

    #[tokio::test]
    async fn abort_after_hard_steer_step_zero_closes_partial_without_injection_or_restart() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        let owner_id = "00000000-0000-4000-8000-000000000071";
        let run_id = "run-abort-hard-steer";
        let turn_id = "turn-owner";
        let (owner_binding, owner_assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (owner_assistant_id, _) = owner_assistant.expect("owner assistant");

        let steer_id = "00000000-0000-4000-8000-000000000072";
        persist_and_pin(&store, &writer, 2, steer_id, "superseded steer").await;
        let abort_id = "00000000-0000-4000-8000-000000000073";
        writer
            .persist_inbound(&test_abort_command(3, abort_id))
            .await
            .expect("persist abort");

        let mut bridge = DurableBridge::new(owner_binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(owner_assistant_id.clone());
        let steer_command = test_admitted(2, steer_id, "superseded steer");
        bridge
            .bind_hard_steer(&writer, steer_command)
            .await
            .expect("commit hard-steer step zero");

        let abort_command = test_admitted_abort(3, abort_id);
        bridge
            .bind_abort(&writer, abort_command)
            .await
            .expect("commit abort cutoff");
        assert!(bridge.pending_hard_steer.is_none());
        assert!(bridge.pending_hard_steer_inject_batch.is_none());

        let partial = PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "test".to_owned(),
            provider: "test".to_owned(),
            origin: crate::provider::types::ProviderOrigin {
                provider_instance_id: "test".to_owned(),
                protocol: crate::provider::types::ApiProtocol::OpenAiChatCompletions,
                model: "test".to_owned(),
            },
            usage: crate::provider::types::Usage::default(),
            stop_reason: StopReason::Aborted,
            error_message: None,
            provider_code: None,
            interrupted: true,
            timestamp: test_timestamp(),
        });

        let (partial_barrier, _) = MessageCommitBarrier::channel();
        let partial_output = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: owner_assistant_id.clone(),
                        message: Box::new(partial.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(partial_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit abort partial assistant");
        assert_eq!(
            partial_output.outputs.len(),
            1,
            "Abort must emit only the original assistant MessageEnd"
        );
        assert!(matches!(
            partial_output.outputs[0].event,
            AgentEvent::MessageEnd { .. }
        ));

        let turn_output = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::TurnEnd {
                        message: Some(Box::new(partial.clone())),
                        tool_results: Vec::new(),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("close aborted turn");
        assert_eq!(turn_output.outputs.len(), 1);
        let end_output = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding,
                    event: AgentEvent::AgentEnd,
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("close aborted run");
        assert_eq!(end_output.outputs.len(), 1);
        assert_eq!(bridge.phase, RunPhase::Finished);

        let events: Vec<String> =
            sqlx::query_scalar("SELECT event_type FROM agent_events ORDER BY seq")
                .fetch_all(store.pool())
                .await
                .expect("read durable events");
        assert!(
            !events.iter().any(|kind| kind == "steered"),
            "Abort must not restart the superseded hard steer"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_events
                 WHERE json_extract(envelope, '$.message_id')=?",
            )
            .bind(crate::store::user_message_id(steer_id))
            .fetch_one(store.pool())
            .await
            .expect("count staged steer injection events"),
            0,
            "superseded hard steer must not inject its user message"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_events
                 WHERE event_type='message_end'
                   AND json_extract(envelope, '$.message_id')=?",
            )
            .bind(owner_assistant_id)
            .fetch_one(store.pool())
            .await
            .expect("count original assistant close"),
            1,
            "Abort must close the original assistant message identity exactly once"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM inbound_commands WHERE command_id=?",
            )
            .bind(steer_id)
            .fetch_one(store.pool())
            .await
            .expect("read superseded steer"),
            "superseded"
        );
    }

    #[tokio::test]
    async fn hard_steer_user_message_consumes_pending_start_and_allows_tool_result() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        let owner_id = "00000000-0000-4000-8000-000000000001";
        let run_id = "run-001";
        let turn_id = "turn-001";
        let (owner_binding, owner_assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (owner_assistant_id, _) = owner_assistant.expect("owner in assistant started");

        let mut bridge = DurableBridge::new(owner_binding);
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(owner_assistant_id.clone());

        let steer_id = "00000000-0000-4000-8000-000000000002";
        let _ = persist_and_pin(&store, &writer, 2, steer_id, "steer now").await;
        let steer_command = test_admitted(2, steer_id, "steer now");

        bridge
            .bind_hard_steer(&writer, steer_command.clone())
            .await
            .expect("bind hard steer");

        let partial = PublicMessage::Assistant(PublicAssistantMessage {
            content: Vec::new(),
            model: "test".to_owned(),
            provider: "test".to_owned(),
            origin: crate::provider::types::ProviderOrigin {
                provider_instance_id: "test".to_owned(),
                protocol: crate::provider::types::ApiProtocol::OpenAiChatCompletions,
                model: "test".to_owned(),
            },
            usage: crate::provider::types::Usage::default(),
            stop_reason: StopReason::Aborted,
            error_message: None,
            provider_code: None,
            interrupted: true,
            timestamp: test_timestamp(),
        });

        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: bridge.binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: owner_assistant_id,
                        message: Box::new(partial),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit hard-steer partial assistant");
        committed.resolve_message_receipts();

        let user_message_id = crate::store::user_message_id(&steer_command.envelope().command_id);
        let user_message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "steer now".to_owned(),
            }],
            timestamp: steer_command.received_at(),
        });

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: bridge.binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: user_message_id.clone(),
                        message: Box::new(user_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit hard-steer user MessageStart");

        let (user_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: bridge.binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: user_message_id,
                        message: Box::new(user_message),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(user_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit hard-steer user MessageEnd");
        committed.resolve_message_receipts();

        let tool_message_id = "tool-result-1".to_owned();
        let tool_message = PublicMessage::ToolResult(ToolResultMessage {
            tool_call_id: "call-1".to_owned(),
            tool_name: "read_file".to_owned(),
            content: vec![UserContent::Text {
                text: "result".to_owned(),
            }],
            details: serde_json::json!({"ok": true}),
            is_error: false,
            timestamp: test_timestamp(),
        });
        let tool_binding = DurableRunBinding {
            command_id: owner_id.to_owned(),
            command_seq: 1,
            run_id: run_id.to_owned(),
            turn_id: bridge.binding.turn_id.clone(),
            executor_generation: bridge.binding.executor_generation,
        };
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: tool_binding,
                    event: AgentEvent::MessageStart {
                        message_id: tool_message_id,
                        message: Box::new(tool_message),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("tool result MessageStart accepted after hard-steer user");
    }

    #[tokio::test]
    async fn error_terminal_preserves_context_durably_without_l0_replay_membership() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        let origin = spec.origin();

        let owner_id = "00000000-0000-4000-8000-000000000079";
        let run_id = "run-error-provider-context";
        let turn_id = "turn-error-provider-context";
        let (owner_binding, owner_assistant) = owner_in_phase_with_origin(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
            origin.clone(),
        )
        .await;
        let (owner_assistant_id, _) = owner_assistant.expect("owner assistant");

        let mut bridge = DurableBridge::new(owner_binding);
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(owner_assistant_id.clone());

        let usage = crate::provider::types::Usage {
            input: 17,
            output: 3,
            total_tokens: 20,
            ..crate::provider::types::Usage::default()
        };
        let error = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
                text: "verified partial".to_owned(),
                wire_item_index: 0,
            }],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin,
            usage,
            stop_reason: StopReason::Error,
            error_message: Some("provider display error".to_owned()),
            provider_code: Some("network_error".to_owned()),
            interrupted: false,
            timestamp: test_timestamp(),
        });
        let fragment = ProviderContextFragment {
            wire_item_index: Some(1),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: serde_json::json!({
                    "id": "rs-error",
                    "type": "reasoning",
                    "summary": [],
                    "encrypted_content": "opaque-error-reasoning",
                }),
            },
        };

        let (barrier, receipt) =
            MessageCommitBarrier::channel_with_provider_context(vec![fragment.clone()]);
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: bridge.binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: owner_assistant_id.clone(),
                        message: Box::new(error.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit authoritative Error terminal");
        let (outputs, _, _, _) = committed.resolve_message_receipts();
        assert!(matches!(
            outputs.as_slice(),
            [CommittedOutput {
                event: AgentEvent::MessageEnd { message, .. },
                ..
            }] if message.as_ref() == &error
        ));
        let receipt = receipt.await.expect("Error MessageEnd receipt");
        assert_eq!(receipt.message_id, owner_assistant_id);

        let provider_row: (String, i64) = sqlx::query_as(
            "SELECT message_id, eviction_tokens
             FROM provider_context
             WHERE message_id = ?",
        )
        .bind(&owner_assistant_id)
        .fetch_one(store.pool())
        .await
        .expect("durable Error provider-context row");
        assert_eq!(provider_row.0, owner_assistant_id);
        assert!(provider_row.1 > 0);

        let membership: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memory_batch_messages WHERE message_id = ?")
                .bind(&owner_assistant_id)
                .fetch_one(store.pool())
                .await
                .expect("count Error L0 membership");
        assert_eq!(membership, 0, "Error assistant must remain outside L0");

        let l0_footprint: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(eviction_footprint_tokens), 0) FROM memory_batches",
        )
        .fetch_one(store.pool())
        .await
        .expect("sum L0 provider-context footprint");
        assert_eq!(
            l0_footprint, 0,
            "durable Error context must not be charged to replayable L0"
        );
    }

    #[tokio::test]
    async fn retry_and_overflow_dispositions_apply_error_context_before_next_attempt() {
        for (shape_index, shape) in [ErrorContextShape::NativeOnly, ErrorContextShape::Mixed]
            .into_iter()
            .enumerate()
        {
            for (delay_index, delay_ms) in [2_000_u64, 0_u64].into_iter().enumerate() {
                let ordinal = 80 + shape_index * 2 + delay_index;
                let owner_id = format!("00000000-0000-4000-8000-{ordinal:012}");
                let run_id = format!("run-error-retry-{ordinal}");
                let turn_id = format!("turn-error-retry-{ordinal}");
                let mut fixture = error_context_fixture(&owner_id, &run_id, &turn_id, shape).await;
                let (barrier, _committed) = RetryWaitCommitBarrier::channel();
                let committed = fixture
                    .bridge
                    .commit(
                        &fixture.writer,
                        RunOutput {
                            binding: fixture.binding.clone(),
                            event: AgentEvent::RetryScheduled {
                                attempt: 1,
                                delay_ms,
                                retry_at: test_timestamp(),
                                error_message: if delay_ms == 0 {
                                    "context overflow".to_owned()
                                } else {
                                    "provider retry".to_owned()
                                },
                            },
                            commit_barrier: None,
                            message_commit_barrier: None,
                            retry_wait_commit_barrier: Some(barrier),
                            approval_command: None,
                            approval_not_started: None,
                            approval_cancelled: None,
                        },
                    )
                    .await
                    .expect("RetryScheduled disposition commits and applies Error Invalidate");
                let (outputs, _, retry_barrier, _) = committed.resolve_message_receipts();
                assert!(matches!(
                    outputs.as_slice(),
                    [CommittedOutput {
                        event: AgentEvent::RetryScheduled { delay_ms: actual, .. },
                        ..
                    }] if *actual == delay_ms
                ));
                retry_barrier
                    .expect("RetryScheduled returns its commit barrier")
                    .committed();
                assert_error_context_applied(&fixture).await;

                let mut next_attempt_message = fixture.message.clone();
                let PublicMessage::Assistant(next_attempt) = &mut next_attempt_message else {
                    unreachable!("fixture assistant")
                };
                next_attempt.content.clear();
                next_attempt.stop_reason = StopReason::Stop;
                next_attempt.error_message = None;
                next_attempt.provider_code = None;
                let next_message_id = format!("{}-next", fixture.message_id);
                fixture
                    .bridge
                    .commit(
                        &fixture.writer,
                        RunOutput {
                            binding: fixture.binding.clone(),
                            event: AgentEvent::MessageStart {
                                message_id: next_message_id,
                                message: Box::new(next_attempt_message),
                            },
                            commit_barrier: None,
                            message_commit_barrier: None,
                            retry_wait_commit_barrier: None,
                            approval_command: None,
                            approval_not_started: None,
                            approval_cancelled: None,
                        },
                    )
                    .await
                    .expect("next attempt starts only after Error context is applied");
            }
        }
    }

    #[tokio::test]
    async fn terminal_error_disposition_applies_context_before_agent_end() {
        for (ordinal, shape) in [
            (86, ErrorContextShape::NativeOnly),
            (87, ErrorContextShape::Mixed),
        ] {
            let mut fixture = error_context_fixture(
                &format!("00000000-0000-4000-8000-{ordinal:012}"),
                &format!("run-terminal-error-context-{ordinal}"),
                &format!("turn-terminal-error-context-{ordinal}"),
                shape,
            )
            .await;
            let committed = fixture
                .bridge
                .commit(
                    &fixture.writer,
                    RunOutput {
                        binding: fixture.binding.clone(),
                        event: AgentEvent::TurnEnd {
                            message: Some(Box::new(fixture.message.clone())),
                            tool_results: Vec::new(),
                        },
                        commit_barrier: None,
                        message_commit_barrier: None,
                        retry_wait_commit_barrier: None,
                        approval_command: None,
                        approval_not_started: None,
                        approval_cancelled: None,
                    },
                )
                .await
                .expect("terminal Error TurnEnd prepares and applies Invalidate");
            assert!(matches!(
                committed.outputs.as_slice(),
                [CommittedOutput {
                    event: AgentEvent::TurnEnd { .. },
                    ..
                }]
            ));
            assert_error_context_applied(&fixture).await;

            fixture
                .bridge
                .commit(
                    &fixture.writer,
                    RunOutput {
                        binding: fixture.binding.clone(),
                        event: AgentEvent::AgentEnd,
                        commit_barrier: None,
                        message_commit_barrier: None,
                        retry_wait_commit_barrier: None,
                        approval_command: None,
                        approval_not_started: None,
                        approval_cancelled: None,
                    },
                )
                .await
                .expect("AgentEnd follows only after terminal Error context is applied");
        }
    }

    #[tokio::test]
    async fn active_abort_cutoff_supersedes_pending_command_and_disposes_error_context() {
        for (ordinal, shape) in [
            (88, ErrorContextShape::NativeOnly),
            (89, ErrorContextShape::Mixed),
        ] {
            let mut fixture = error_context_fixture(
                &format!("00000000-0000-4000-8000-{ordinal:012}"),
                &format!("run-abort-error-context-{ordinal}"),
                &format!("turn-abort-error-context-{ordinal}"),
                shape,
            )
            .await;
            let superseded_id = "00000000-0000-4000-8000-000000000084";
            persist_and_pin(
                &fixture.store,
                &fixture.writer,
                2,
                superseded_id,
                "superseded while Error closes",
            )
            .await;
            let abort_id = "00000000-0000-4000-8000-000000000085";
            fixture
                .writer
                .persist_inbound(&test_abort_command(3, abort_id))
                .await
                .expect("persist shared Abort cutoff");

            fixture
                .bridge
                .bind_abort(&fixture.writer, test_admitted_abort(3, abort_id))
                .await
                .expect("Abort cutoff atomically supersedes and prepares Error disposition");
            assert_error_context_applied(&fixture).await;
            assert_eq!(
                sqlx::query_scalar::<_, String>(
                    "SELECT status FROM inbound_commands WHERE command_id = ?",
                )
                .bind(superseded_id)
                .fetch_one(fixture.store.pool())
                .await
                .expect("read superseded command"),
                "superseded"
            );
            assert_eq!(fixture.bridge.phase, RunPhase::CancelRequested);

            fixture
                .bridge
                .commit(
                    &fixture.writer,
                    RunOutput {
                        binding: fixture.binding.clone(),
                        event: AgentEvent::TurnEnd {
                            message: Some(Box::new(fixture.message.clone())),
                            tool_results: Vec::new(),
                        },
                        commit_barrier: None,
                        message_commit_barrier: None,
                        retry_wait_commit_barrier: None,
                        approval_command: None,
                        approval_not_started: None,
                        approval_cancelled: None,
                    },
                )
                .await
                .expect("aborted Error attempt closes after shared disposition");
            fixture
                .bridge
                .commit(
                    &fixture.writer,
                    RunOutput {
                        binding: fixture.binding.clone(),
                        event: AgentEvent::AgentEnd,
                        commit_barrier: None,
                        message_commit_barrier: None,
                        retry_wait_commit_barrier: None,
                        approval_command: None,
                        approval_not_started: None,
                        approval_cancelled: None,
                    },
                )
                .await
                .expect("aborted run closes after Error context reaches applied");
        }
    }

    #[tokio::test]
    async fn hard_steer_partial_preserves_provider_context_and_eviction_footprint() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let spec = ModelSpec::preset("openai-responses").expect("Responses preset");
        let origin = spec.origin();

        let owner_id = "00000000-0000-4000-8000-000000000081";
        let run_id = "run-hard-steer-provider-context";
        let turn_id = "turn-hard-steer-provider-context";
        let (owner_binding, owner_assistant) = owner_in_phase_with_origin(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
            origin.clone(),
        )
        .await;
        let (owner_assistant_id, _) = owner_assistant.expect("owner assistant");

        let mut bridge = DurableBridge::new(owner_binding);
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(owner_assistant_id.clone());

        let steer_id = "00000000-0000-4000-8000-000000000082";
        persist_and_pin(&store, &writer, 2, steer_id, "steer with provider context").await;
        bridge
            .bind_hard_steer(
                &writer,
                test_admitted(2, steer_id, "steer with provider context"),
            )
            .await
            .expect("bind hard steer");

        let partial = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
                text: "verified partial".to_owned(),
                wire_item_index: 0,
            }],
            model: spec.id.clone(),
            provider: spec.provider.clone(),
            origin: origin.clone(),
            usage: crate::provider::types::Usage::default(),
            stop_reason: StopReason::Aborted,
            error_message: None,
            provider_code: None,
            interrupted: true,
            timestamp: test_timestamp(),
        });
        let fragment = ProviderContextFragment {
            wire_item_index: Some(1),
            payload: ProviderContextPayload::EncryptedReasoning {
                protocol: ApiProtocol::OpenAiResponses,
                item: serde_json::json!({
                    "id": "rs-hard-steer",
                    "type": "reasoning",
                    "summary": [],
                    "encrypted_content": "opaque-hard-steer-reasoning",
                }),
            },
        };
        let expected_footprint = eviction_footprint_for_payload(&spec, &fragment.payload)
            .expect("provider context footprint");
        let expected_footprint_tokens = expected_footprint.eviction_tokens();
        assert!(expected_footprint_tokens > 0);

        let (assistant_barrier, assistant_receipt) =
            MessageCommitBarrier::channel_with_provider_context(vec![fragment.clone()]);
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: bridge.binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: owner_assistant_id.clone(),
                        message: Box::new(partial.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit hard-steer partial assistant");
        assert_eq!(committed.outputs.len(), 2);

        let receipt = assistant_receipt
            .await
            .expect("hard-steer MessageEnd receipt");
        assert_eq!(receipt.message_id, owner_assistant_id);
        assert!(receipt.new_turn_id.is_some());

        let provider_row = sqlx::query_as::<_, (String, i64, i64, i64, i64)>(
            "SELECT message_id, message_seq, wire_item_index, item_ordinal, eviction_tokens
             FROM provider_context",
        )
        .fetch_one(store.pool())
        .await
        .expect("hard-steer provider-context row");
        assert_eq!(
            provider_row,
            (
                owner_assistant_id.clone(),
                i64::try_from(receipt.message_seq).expect("message seq fits SQLite"),
                1,
                0,
                i64::try_from(expected_footprint_tokens).expect("footprint fits SQLite"),
            )
        );

        let durable_footprint: i64 = sqlx::query_scalar(
            "SELECT batches.eviction_footprint_tokens
             FROM memory_batches AS batches
             JOIN memory_batch_messages AS members ON members.batch_id = batches.id
             WHERE members.message_id = ?",
        )
        .bind(&owner_assistant_id)
        .fetch_one(store.pool())
        .await
        .expect("hard-steer partial L0 footprint");
        assert_eq!(
            durable_footprint,
            i64::try_from(expected_footprint_tokens).expect("footprint fits SQLite")
        );

        let normalized =
            normalize_partial_assistant(partial).expect("normalize hard-steer partial assistant");
        let expected_context = vec![ProviderContextItemWithFootprint::new(
            ProviderContextItem {
                retention_owner: ProviderContextAnchor {
                    message_id: owner_assistant_id.clone(),
                    message_seq: receipt.message_seq,
                },
                origin_message: Some(ProviderContextAnchor {
                    message_id: owner_assistant_id.clone(),
                    message_seq: receipt.message_seq,
                }),
                wire_item_index: fragment.wire_item_index,
                ordinal: 0,
                provider_origin: origin,
                payload: fragment.payload,
            },
            expected_footprint,
        )];
        let messages = vec![ContextMessage::Persisted {
            id: owner_assistant_id,
            seq: receipt.message_seq,
            message: Message::from(normalized),
        }];
        let mut transaction = store.pool().begin().await.expect("replay transaction");
        let replayed = store
            .hydrate_provider_context(&messages, &mut transaction)
            .await
            .expect("authenticate and replay hard-steer provider context");
        transaction
            .rollback()
            .await
            .expect("rollback replay transaction");
        assert_eq!(replayed, expected_context);
    }

    #[tokio::test]
    async fn pending_tool_calls_classify_as_tool_or_approval_for_soft_steer() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000201";
        let run_id = "run-1";
        let turn_id = "turn-1";
        let (binding, _) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;

        let mut bridge = DurableBridge::new(binding);
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = None;
        bridge.pending_tool_calls.insert("pending-call".to_owned());

        assert_eq!(
            bridge.steer_stage(),
            SteerStage::ToolOrApproval,
            "commands between assistant MessageEnd and tool start must be ToolOrApproval"
        );

        let steer_id = "00000000-0000-4000-8000-000000000202";
        persist_and_pin(&store, &writer, 2, steer_id, "steer now").await;
        let command = test_admitted(2, steer_id, "steer now");
        assert!(
            bridge.can_bind_soft_steer(&writer, &command),
            "soft steer must bind while tool calls are pending"
        );
        bridge
            .bind_soft_steer(&writer, command.clone())
            .await
            .expect("bind soft steer in pending-tool-calls window");
    }

    #[tokio::test]
    async fn soft_steer_turn_start_preserves_old_turn_until_group_commit() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000301";
        let run_id = "run-1";
        let old_turn_id = "turn-1";
        let (owner_binding, assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            old_turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, assistant_base) = assistant.expect("assistant MessageStart");

        let tool_call = ToolCall {
            id: "soft-call".to_owned(),
            name: "fixture-tool".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(
                serde_json::json!({"safe": true}),
            )
            .expect("validated arguments"),
        };
        let mut assistant_with_tool = match assistant_base {
            PublicMessage::Assistant(a) => a,
            _ => unreachable!(),
        };
        assistant_with_tool.content = vec![PublicAssistantContent::ToolCall {
            tool_call: tool_call.clone(),
            wire_item_index: 0,
        }];
        assistant_with_tool.stop_reason = StopReason::Stop;
        let assistant_message = PublicMessage::Assistant(assistant_with_tool);

        let mut bridge = DurableBridge::new(owner_binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());

        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id.clone(),
                        message: Box::new(assistant_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit assistant MessageEnd with tool call");
        committed.resolve_message_receipts();

        let (tool_start_barrier, _) = ToolStartCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::ToolExecutionStart {
                        tool_call_id: tool_call.id.clone(),
                        tool_name: tool_call.name.clone(),
                        args: serde_json::json!({"safe": true}),
                    },
                    commit_barrier: Some(tool_start_barrier),
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool execution start");
        let (_, tool_barrier, _, _) = committed.resolve_message_receipts();
        if let Some(barrier) = tool_barrier {
            barrier.committed();
        }

        let steer_id = "00000000-0000-4000-8000-000000000302";
        persist_and_pin(&store, &writer, 2, steer_id, "steer now").await;
        let steer_command = test_admitted(2, steer_id, "steer now");
        bridge
            .bind_soft_steer(&writer, steer_command)
            .await
            .expect("bind soft steer after tool call pending");

        let result_message = ToolResultMessage {
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name.clone(),
            content: vec![UserContent::Text {
                text: "ok".to_owned(),
            }],
            details: serde_json::json!({"ok": true}),
            is_error: false,
            timestamp: test_timestamp(),
        };
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::ToolExecutionEnd {
                        tool_call_id: tool_call.id.clone(),
                        result: serde_json::to_value(&result_message).expect("serialize result"),
                        is_error: false,
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool execution end");

        let tool_result_id = "soft-result".to_owned();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: tool_result_id.clone(),
                        message: Box::new(PublicMessage::ToolResult(result_message.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool result MessageStart");

        let (tool_result_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: tool_result_id,
                        message: Box::new(PublicMessage::ToolResult(result_message.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(tool_result_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool result MessageEnd");
        committed.resolve_message_receipts();

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant_message.clone())),
                        tool_results: vec![result_message.clone()],
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("buffer old turn end while steer group is pending");

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::TurnStart,
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("buffer new turn start while steer group is pending");

        let user_message_id = crate::store::user_message_id(steer_id);
        let user_message = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: "steer now".to_owned(),
            }],
            timestamp: test_timestamp(),
        });
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: user_message_id.clone(),
                        message: Box::new(user_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("collect steer group user MessageStart");

        let (user_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: user_message_id,
                        message: Box::new(user_message),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(user_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit steer group and complete injection");
        committed.resolve_message_receipts();

        let turn_end_turn_id: String = sqlx::query_scalar(
            "SELECT json_extract(internal_metadata, '$.turn_id') FROM agent_events
             WHERE event_type='turn_end' ORDER BY seq DESC LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("turn end turn id");
        let turn_start_turn_id: String = sqlx::query_scalar(
            "SELECT json_extract(internal_metadata, '$.turn_id') FROM agent_events
             WHERE event_type='turn_start' ORDER BY seq DESC LIMIT 1",
        )
        .fetch_one(store.pool())
        .await
        .expect("turn start turn id");

        assert_eq!(
            turn_end_turn_id, old_turn_id,
            "old TurnEnd must use the original turn"
        );
        assert_ne!(
            turn_start_turn_id, old_turn_id,
            "new TurnStart must introduce the group turn"
        );
    }

    #[tokio::test]
    async fn abort_after_length_guard_skips_not_started_as_user_steer_cancelled() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000401";
        let run_id = "run-1";
        let turn_id = "turn-1";
        let (owner_binding, assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, assistant_base) = assistant.expect("assistant MessageStart");

        let length_call = ToolCall {
            id: "length-call".to_owned(),
            name: "fixture-tool".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(
                serde_json::json!({"safe": true}),
            )
            .expect("validated arguments"),
        };
        let mut length_assistant = match assistant_base {
            PublicMessage::Assistant(a) => a,
            _ => unreachable!(),
        };
        length_assistant.content = vec![PublicAssistantContent::ToolCall {
            tool_call: length_call.clone(),
            wire_item_index: 0,
        }];
        length_assistant.stop_reason = StopReason::Length;
        let length_assistant_message = PublicMessage::Assistant(length_assistant);

        let mut bridge = DurableBridge::new(owner_binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());

        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id.clone(),
                        message: Box::new(length_assistant_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit length-guarded assistant MessageEnd");
        committed.resolve_message_receipts();

        let abort_id = "00000000-0000-4000-8000-000000000402";
        writer
            .persist_inbound(&test_abort_command(2, abort_id))
            .await
            .expect("persist abort command");
        let abort_command = test_admitted_abort(2, abort_id);
        bridge
            .bind_abort(&writer, abort_command)
            .await
            .expect("bind abort after length guard");

        let result_message = ToolResultMessage {
            tool_call_id: length_call.id.clone(),
            tool_name: length_call.name.clone(),
            content: vec![UserContent::Text {
                text: "not executed".to_owned(),
            }],
            details: serde_json::json!({"error": "length_guard"}),
            is_error: true,
            timestamp: test_timestamp(),
        };
        let result_id = "length-result".to_owned();

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: result_id.clone(),
                        message: Box::new(PublicMessage::ToolResult(result_message.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit length result MessageStart");

        let (result_barrier, _) = MessageCommitBarrier::channel();
        let committed = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: result_id,
                        message: Box::new(PublicMessage::ToolResult(result_message.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(result_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit length result MessageEnd with Skip");
        committed.resolve_message_receipts();

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::TurnEnd {
                        message: Some(Box::new(length_assistant_message.clone())),
                        tool_results: vec![result_message.clone()],
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit TurnEnd after abort");

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: owner_binding.clone(),
                    event: AgentEvent::AgentEnd,
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit AgentEnd after abort");

        let row: (String, String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT state, command_id, started_at, error_code
             FROM tool_executions WHERE tool_call_id = ?",
        )
        .bind(&length_call.id)
        .fetch_one(store.pool())
        .await
        .expect("length tool execution row");
        assert_eq!(row.0, "not_started", "tool must remain not_started");
        assert_eq!(row.1, owner_id, "tool must belong to the original owner");
        assert!(row.2.is_none(), "not_started tool must have no started_at");
        assert_eq!(
            row.3.as_deref(),
            Some("user_steer_cancelled"),
            "abort must record user_steer_cancelled, not length_guard"
        );

        let execution_lifecycle: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_events
             WHERE event_type IN ('tool_execution_start', 'tool_execution_end', 'retry_scheduled')",
        )
        .fetch_one(store.pool())
        .await
        .expect("execution lifecycle count");
        assert_eq!(
            execution_lifecycle, 0,
            "aborted length call must not execute or retry"
        );

        let owner_status: (String, String) =
            sqlx::query_as("SELECT status, run_phase FROM inbound_commands WHERE command_id = ?")
                .bind(owner_id)
                .fetch_one(store.pool())
                .await
                .expect("owner status");
        assert_eq!(owner_status.0, "applied", "original owner must close");
        assert_eq!(
            owner_status.1, "finished",
            "original owner must be finished"
        );
    }

    #[tokio::test]
    async fn policy_denial_is_not_swallowed_by_pending_tool_cleanup() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000451";
        let run_id = "run-policy-denial";
        let turn_id = "turn-policy-denial";
        let (binding, assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, assistant_base) = assistant.expect("assistant MessageStart");
        let tool_call = ToolCall {
            id: "policy-denied-call".to_owned(),
            name: "fixture-tool".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(
                serde_json::json!({"safe": true}),
            )
            .expect("validated arguments"),
        };
        let mut assistant = match assistant_base {
            PublicMessage::Assistant(assistant) => assistant,
            _ => unreachable!(),
        };
        assistant.content = vec![PublicAssistantContent::ToolCall {
            tool_call: tool_call.clone(),
            wire_item_index: 0,
        }];
        assistant.stop_reason = StopReason::Stop;
        let assistant = PublicMessage::Assistant(assistant);

        let mut bridge = DurableBridge::new(binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());
        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id,
                        message: Box::new(assistant),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit assistant tool call")
            .resolve_message_receipts();

        let result = ToolResultMessage {
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name,
            content: vec![UserContent::Text {
                text: "policy denied".to_owned(),
            }],
            details: serde_json::json!({"error": "approval_denied"}),
            is_error: true,
            timestamp: test_timestamp(),
        };
        let result_id = "policy-denied-result".to_owned();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: result_id.clone(),
                        message: Box::new(PublicMessage::ToolResult(result.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("buffer denied result start");
        let (result_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding,
                    event: AgentEvent::MessageEnd {
                        message_id: result_id,
                        message: Box::new(PublicMessage::ToolResult(result)),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(result_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: Some(tool_call.id.clone()),
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit policy denial")
            .resolve_message_receipts();

        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>, i64)>(
                "SELECT state, error_code,
                    (SELECT COUNT(*) FROM agent_events
                     WHERE event_type='tool_execution_start'
                       AND json_extract(envelope, '$.tool_call_id')=?)
                 FROM tool_executions WHERE tool_call_id=?",
            )
            .bind(&tool_call.id)
            .bind(&tool_call.id)
            .fetch_one(store.pool())
            .await
            .expect("policy denial durable state"),
            (
                "not_started".to_owned(),
                Some("approval_denied".to_owned()),
                0
            )
        );
    }

    #[tokio::test]
    async fn bind_abort_clears_pending_soft_steer_group_state() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        let owner_id = "00000000-0000-4000-8000-000000000501";
        let run_id = "run-1";
        let turn_id = "turn-1";
        let (owner_binding, _) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;

        let mut bridge = DurableBridge::new(owner_binding);
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = None;
        bridge.pending_tool_calls.insert("tool-1".to_owned());

        let steer_id = "00000000-0000-4000-8000-000000000502";
        persist_and_pin(&store, &writer, 2, steer_id, "steer now").await;
        let steer_command = test_admitted(2, steer_id, "steer now");
        bridge
            .bind_soft_steer(&writer, steer_command)
            .await
            .expect("bind soft steer");

        assert!(bridge.pending_steer_group.is_some());
        bridge.pending_steer_turn_end = Some((
            PublicMessage::Assistant(PublicAssistantMessage {
                content: Vec::new(),
                model: "test".to_owned(),
                provider: "test".to_owned(),
                origin: crate::provider::types::ProviderOrigin {
                    provider_instance_id: "test".to_owned(),
                    protocol: crate::provider::types::ApiProtocol::OpenAiChatCompletions,
                    model: "test".to_owned(),
                },
                usage: crate::provider::types::Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                provider_code: None,
                interrupted: false,
                timestamp: test_timestamp(),
            }),
            Vec::new(),
        ));
        bridge.pending_steer_turn_start = true;
        bridge.pending_steer_collecting = true;
        let (barrier, _) = MessageCommitBarrier::channel();
        bridge.pending_steer_messages.push(PendingSteerMessage {
            message_id: "pending-start".to_owned(),
            message: PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "pending".to_owned(),
                }],
                timestamp: test_timestamp(),
            }),
            barrier,
        });
        bridge.pending_steer_open_start = Some((
            "open-start".to_owned(),
            PublicMessage::User(UserMessage {
                content: vec![UserContent::Text {
                    text: "open".to_owned(),
                }],
                timestamp: test_timestamp(),
            }),
        ));

        let abort_id = "00000000-0000-4000-8000-000000000503";
        writer
            .persist_inbound(&test_abort_command(3, abort_id))
            .await
            .expect("persist abort");
        let abort_command = test_admitted_abort(3, abort_id);
        bridge
            .bind_abort(&writer, abort_command)
            .await
            .expect("bind abort");

        assert!(bridge.pending_steer_group.is_none());
        assert!(bridge.pending_steer_turn_end.is_none());
        assert!(!bridge.pending_steer_turn_start);
        assert!(!bridge.pending_steer_collecting);
        assert!(bridge.pending_steer_messages.is_empty());
        assert!(bridge.pending_steer_open_start.is_none());
    }

    #[tokio::test]
    async fn agent_end_rejects_pending_approved_resolution_without_tool_start() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let (binding, _) = owner_in_phase(
            &store,
            &writer,
            "00000000-0000-4000-8000-000000000001",
            "run-pending",
            "turn-1",
            RunPhase::AssistantStarted,
        )
        .await;
        let mut bridge = DurableBridge::new(binding);

        let decision_command = AdmittedCommand::new(
            CommandEnvelope {
                seq: 2,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000002")
                    .expect("canonical test UUID"),
                command: Command::ApprovalDecision {
                    request_id: "request-1".to_owned(),
                    decision: ApprovalDecision::ApproveOnce,
                },
            },
            test_timestamp(),
        );
        bridge.pending_approval_resolved = vec![(
            "request-1".to_owned(),
            ApprovalResolution::Decision(ApprovalDecision::ApproveOnce),
            decision_command,
        )];

        let result = bridge
            .commit(
                &writer,
                RunOutput::detached(bridge.binding.clone(), AgentEvent::AgentEnd, None),
            )
            .await;
        assert!(
            result.is_err_and(|e| e.to_string().contains("approved tools have not started")),
            "AgentEnd must not commit an approved decision before ToolExecutionStart"
        );
    }

    #[tokio::test]
    async fn two_queued_approval_resolutions_are_consumed_in_order() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let (binding, _) = owner_in_phase(
            &store,
            &writer,
            "00000000-0000-4000-8000-000000000001",
            "run-pending",
            "turn-1",
            RunPhase::AssistantStarted,
        )
        .await;
        let mut bridge = DurableBridge::new(binding);
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = None;

        let decision_command = |seq: u64, request_id: &str| {
            AdmittedCommand::new(
                CommandEnvelope {
                    seq,
                    command_id: CommandId::parse(&format!("00000000-0000-4000-8000-{seq:012}"))
                        .expect("canonical test UUID"),
                    command: Command::ApprovalDecision {
                        request_id: request_id.to_owned(),
                        decision: ApprovalDecision::ApproveOnce,
                    },
                },
                test_timestamp(),
            )
        };

        for (seq, request_id) in [(2u64, "request-1"), (3u64, "request-2")] {
            bridge
                .commit(
                    &writer,
                    RunOutput {
                        binding: bridge.binding.clone(),
                        event: AgentEvent::ApprovalResolved {
                            request_id: request_id.to_owned(),
                            resolution: ApprovalResolution::Decision(ApprovalDecision::ApproveOnce),
                        },
                        commit_barrier: None,
                        message_commit_barrier: None,
                        retry_wait_commit_barrier: None,
                        approval_command: Some(decision_command(seq, request_id)),
                        approval_not_started: None,
                        approval_cancelled: None,
                    },
                )
                .await
                .expect("commit ApprovalResolved approve");
        }

        assert_eq!(bridge.pending_approval_resolved.len(), 2);
        assert_eq!(bridge.pending_approval_resolved[0].0, "request-1");
        assert_eq!(bridge.pending_approval_resolved[1].0, "request-2");

        bridge.turn_open = false;
        let result = bridge
            .commit(
                &writer,
                RunOutput::detached(bridge.binding.clone(), AgentEvent::AgentEnd, None),
            )
            .await;
        assert!(
            result.is_err_and(|e| e.to_string().contains("approved tools have not started")),
            "AgentEnd must not commit while approved resolutions are still queued"
        );
    }

    #[tokio::test]
    async fn tool_execution_start_dropped_when_superseded_by_soft_steer() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        let (mut bridge, binding, _assistant_message, tool_call) = setup_pending_approval(
            &store,
            &writer,
            "00000000-0000-4000-8000-000000000001",
            "run-soft-steer",
            "turn-1",
            "tool-call-1",
            "request-1",
        )
        .await;

        let decision_command = AdmittedCommand::new(
            CommandEnvelope {
                seq: 3,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000003")
                    .expect("canonical test UUID"),
                command: Command::ApprovalDecision {
                    request_id: "request-1".to_owned(),
                    decision: ApprovalDecision::ApproveOnce,
                },
            },
            test_timestamp(),
        );
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ApprovalResolved {
                        request_id: "request-1".to_owned(),
                        resolution: ApprovalResolution::Decision(ApprovalDecision::ApproveOnce),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: Some(decision_command),
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit ApprovalResolved approve");

        let steer_command = AdmittedCommand::new(
            CommandEnvelope {
                seq: 2,
                command_id: CommandId::parse("00000000-0000-4000-8000-000000000002")
                    .expect("canonical test UUID"),
                command: Command::UserMessage {
                    text: "stop".to_owned(),
                    attachments: Vec::new(),
                },
            },
            test_timestamp(),
        );
        let mut group = SteerGroup::new(
            ApplicationKind::SoftSteer,
            binding.run_id.clone(),
            binding.turn_id.clone(),
        )
        .expect("create soft steer group");
        group
            .push(steer_command, writer.store().redactor())
            .expect("push steer command");
        bridge.pending_steer_group = Some(group);
        bridge.pending_steer_collecting = true;

        let (barrier, _barrier_rx) = ToolStartCommitBarrier::channel();
        let dropped = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ToolExecutionStart {
                        tool_call_id: tool_call.id.clone(),
                        tool_name: tool_call.name.clone(),
                        args: serde_json::to_value(&tool_call.arguments).unwrap_or_default(),
                    },
                    commit_barrier: Some(barrier),
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit dropped ToolExecutionStart");
        assert!(dropped.outputs.is_empty());
        assert_eq!(bridge.pending_approval_resolved.len(), 1);

        bridge
            .commit(
                &writer,
                RunOutput::detached(
                    binding.clone(),
                    AgentEvent::ApprovalResolved {
                        request_id: "request-1".to_owned(),
                        resolution: ApprovalResolution::Cancelled,
                    },
                    None,
                ),
            )
            .await
            .expect("commit ApprovalResolved Cancelled");
        assert!(bridge.pending_approval_resolved.is_empty());
        let (request_id, command) = bridge
            .pending_cancelled_approval_commands
            .get("tool-call-1")
            .expect("cancelled approved command remains staged until its result commits");
        assert_eq!(request_id, "request-1");
        assert_eq!(command.envelope().seq, 3);
        assert!(
            !bridge.approval_cancelled.contains("tool-call-1"),
            "the worker's cancelled ToolResult marker selects the atomic terminal commit"
        );
    }

    /// If a soft-steer group is durably bound before the matching
    /// `ToolExecutionStart` is committed, the bridge must drop the start so the
    /// durable history does not claim a started tool that was preempted.
    #[tokio::test]
    async fn tool_execution_start_dropped_when_soft_steer_bound_before_start() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000901";
        let run_id = "run-soft-steer-bound";
        let turn_id = "turn-1";

        let (binding, assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, assistant_base) = assistant.expect("assistant MessageStart");
        let tool_call = ToolCall {
            id: "soft-bound-call".to_owned(),
            name: "fixture-tool".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(
                serde_json::json!({"safe": true}),
            )
            .expect("validated arguments"),
        };
        let mut assistant_with_tool = match assistant_base {
            PublicMessage::Assistant(a) => a,
            _ => unreachable!(),
        };
        assistant_with_tool.content = vec![PublicAssistantContent::ToolCall {
            tool_call: tool_call.clone(),
            wire_item_index: 0,
        }];
        assistant_with_tool.stop_reason = StopReason::Stop;
        let assistant_message = PublicMessage::Assistant(assistant_with_tool);

        let mut bridge = DurableBridge::new(binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());

        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id.clone(),
                        message: Box::new(assistant_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit assistant MessageEnd with tool call")
            .resolve_message_receipts();

        bridge.assistant_open = None;
        bridge.pending_tool_calls.insert(tool_call.id.clone());

        let steer_id = "00000000-0000-4000-8000-000000000902";
        persist_and_pin(&store, &writer, 2, steer_id, "steer now").await;
        let steer_command = test_admitted(2, steer_id, "steer now");
        bridge
            .bind_soft_steer(&writer, steer_command)
            .await
            .expect("bind soft steer before tool start");

        let (barrier, _) = ToolStartCommitBarrier::channel();
        let dropped = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ToolExecutionStart {
                        tool_call_id: tool_call.id.clone(),
                        tool_name: tool_call.name.clone(),
                        args: serde_json::json!({"safe": true}),
                    },
                    commit_barrier: Some(barrier),
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit dropped ToolExecutionStart");
        assert!(dropped.outputs.is_empty(), "start must be dropped");

        let start_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_events WHERE event_type='tool_execution_start'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count tool_execution_start events");
        assert_eq!(
            start_count, 0,
            "durable history must not contain a tool_execution_start when soft steer bound first"
        );
    }

    /// Same complement as above, but for an abort bound before the
    /// `ToolExecutionStart` is committed.
    #[tokio::test]
    async fn tool_execution_start_dropped_when_abort_bound_before_start() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());
        let owner_id = "00000000-0000-4000-8000-000000000911";
        let run_id = "run-abort-bound";
        let turn_id = "turn-1";

        let (binding, assistant) = owner_in_phase(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, assistant_base) = assistant.expect("assistant MessageStart");
        let tool_call = ToolCall {
            id: "abort-bound-call".to_owned(),
            name: "fixture-tool".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(
                serde_json::json!({"safe": true}),
            )
            .expect("validated arguments"),
        };
        let mut assistant_with_tool = match assistant_base {
            PublicMessage::Assistant(a) => a,
            _ => unreachable!(),
        };
        assistant_with_tool.content = vec![PublicAssistantContent::ToolCall {
            tool_call: tool_call.clone(),
            wire_item_index: 0,
        }];
        assistant_with_tool.stop_reason = StopReason::Stop;
        let assistant_message = PublicMessage::Assistant(assistant_with_tool);

        let mut bridge = DurableBridge::new(binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());

        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id.clone(),
                        message: Box::new(assistant_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit assistant MessageEnd with tool call")
            .resolve_message_receipts();

        bridge.assistant_open = None;
        bridge.pending_tool_calls.insert(tool_call.id.clone());

        let abort_id = "00000000-0000-4000-8000-000000000912";
        writer
            .persist_inbound(&test_abort_command(2, abort_id))
            .await
            .expect("persist abort");
        let abort_command = test_admitted_abort(2, abort_id);
        bridge
            .bind_abort(&writer, abort_command)
            .await
            .expect("bind abort before tool start");

        let (barrier, _) = ToolStartCommitBarrier::channel();
        let dropped = bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ToolExecutionStart {
                        tool_call_id: tool_call.id.clone(),
                        tool_name: tool_call.name.clone(),
                        args: serde_json::json!({"safe": true}),
                    },
                    commit_barrier: Some(barrier),
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit dropped ToolExecutionStart");
        assert!(dropped.outputs.is_empty(), "start must be dropped");

        let start_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM agent_events WHERE event_type='tool_execution_start'",
        )
        .fetch_one(store.pool())
        .await
        .expect("count tool_execution_start events");
        assert_eq!(
            start_count, 0,
            "durable history must not contain a tool_execution_start when abort bound first"
        );
    }

    #[test]
    fn steer_stage_treats_approval_prepared_tools_as_tool_or_approval() {
        let mut bridge = DurableBridge::new(binding("00000000-0000-4000-8000-000000000001"));
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = None;
        bridge
            .approval_prepared_tools
            .insert("tool-call-1".to_owned());
        assert_eq!(bridge.steer_stage(), SteerStage::ToolOrApproval);
    }

    async fn setup_pending_approval(
        store: &Store,
        writer: &EventWriter,
        owner_id: &str,
        run_id: &str,
        turn_id: &str,
        tool_call_id: &str,
        request_id: &str,
    ) -> (DurableBridge, DurableRunBinding, PublicMessage, ToolCall) {
        let (binding, assistant) = owner_in_phase(
            store,
            writer,
            owner_id,
            run_id,
            turn_id,
            RunPhase::AssistantStarted,
        )
        .await;
        let (assistant_id, assistant_base) = assistant.expect("assistant MessageStart");
        let mut assistant = match assistant_base {
            PublicMessage::Assistant(a) => a,
            _ => unreachable!(),
        };
        let tool_call = ToolCall {
            id: tool_call_id.to_owned(),
            name: "bash".to_owned(),
            arguments: serde_json::from_value::<ValidatedToolArguments>(
                serde_json::json!({"command": "echo hello", "description": "test"}),
            )
            .expect("validated arguments"),
        };
        assistant.content = vec![PublicAssistantContent::ToolCall {
            tool_call: tool_call.clone(),
            wire_item_index: 0,
        }];
        assistant.stop_reason = StopReason::Stop;
        let assistant_message = PublicMessage::Assistant(assistant);

        let mut bridge = DurableBridge::new(binding.clone());
        bridge.phase = RunPhase::AssistantStarted;
        bridge.turn_open = true;
        bridge.assistant_open = Some(assistant_id.clone());

        let (assistant_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: assistant_id.clone(),
                        message: Box::new(assistant_message.clone()),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(assistant_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit assistant MessageEnd with tool call")
            .resolve_message_receipts();

        let request = ApprovalRequest {
            id: request_id.to_owned(),
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name.clone(),
            action: ReviewProjection::Reviewable(serde_json::json!({
                "command": "echo hello",
                "argv": ["echo", "hello"],
            })),
            args_summary: serde_json::json!({}),
            reason: Some("bash requires approval".to_owned()),
            audit: None,
        };
        bridge
            .commit(
                writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ApprovalRequested { request },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit ApprovalRequested");

        assert!(bridge.approval_prepared_tools.contains(tool_call_id));
        (bridge, binding, assistant_message, tool_call)
    }

    #[tokio::test]
    async fn approval_denied_releases_prepared_tools_and_allows_agent_end() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        let owner_id = "00000000-0000-4000-8000-000000000801";
        let run_id = "run-denied-cleanup";
        let turn_id = "turn-denied-cleanup";
        let tool_call_id = "denied-call";
        let request_id = "request-denied";

        let (mut bridge, binding, assistant_message, tool_call) = setup_pending_approval(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            tool_call_id,
            request_id,
        )
        .await;

        let decision_id = "00000000-0000-4000-8000-000000000802";
        let decision_command =
            test_admitted_approval_decision(2, decision_id, request_id, ApprovalDecision::Deny);
        writer
            .persist_inbound(&InboundCommand::Valid(decision_command.envelope().clone()))
            .await
            .expect("persist approval decision");

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ApprovalResolved {
                        request_id: request_id.to_owned(),
                        resolution: ApprovalResolution::Decision(ApprovalDecision::Deny),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: Some(decision_command),
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit ApprovalResolved Deny");

        let result = ToolResultMessage {
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name.clone(),
            content: Vec::new(),
            details: serde_json::json!({"error": "approval_denied"}),
            is_error: true,
            timestamp: test_timestamp(),
        };
        let result_for_turn = result.clone();
        let result_id = "denied-result".to_owned();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: result_id.clone(),
                        message: Box::new(PublicMessage::ToolResult(result.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool result MessageStart");

        let (result_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: result_id,
                        message: Box::new(PublicMessage::ToolResult(result)),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(result_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: Some(tool_call.id.clone()),
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool result MessageEnd")
            .resolve_message_receipts();

        assert!(
            bridge.approval_prepared_tools.is_empty(),
            "approval_prepared_tools must be released after denied ToolResult"
        );

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant_message.clone())),
                        tool_results: vec![result_for_turn],
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("close denied turn");

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding,
                    event: AgentEvent::AgentEnd,
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("AgentEnd after denied approval");
        assert_eq!(bridge.phase, RunPhase::Finished);

        let (state, error_code, start_count) = sqlx::query_as::<_, (String, Option<String>, i64)>(
            "SELECT state, error_code,
                (SELECT COUNT(*) FROM agent_events WHERE event_type='tool_execution_start'
                 AND json_extract(envelope, '$.tool_call_id')=?)
             FROM tool_executions WHERE tool_call_id=?",
        )
        .bind(&tool_call.id)
        .bind(&tool_call.id)
        .fetch_one(store.pool())
        .await
        .expect("read denied tool execution");
        assert_eq!(state, "cancelled");
        assert_eq!(error_code.as_deref(), Some("approval_denied"));
        assert_eq!(
            start_count, 0,
            "denied approval must not emit ToolExecutionStart"
        );
    }

    #[tokio::test]
    async fn approval_cancelled_releases_prepared_tools_and_allows_agent_end() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        let owner_id = "00000000-0000-4000-8000-000000000901";
        let run_id = "run-cancelled-cleanup";
        let turn_id = "turn-cancelled-cleanup";
        let tool_call_id = "cancelled-call";
        let request_id = "request-cancelled";

        let (mut bridge, binding, assistant_message, tool_call) = setup_pending_approval(
            &store,
            &writer,
            owner_id,
            run_id,
            turn_id,
            tool_call_id,
            request_id,
        )
        .await;

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::ApprovalResolved {
                        request_id: request_id.to_owned(),
                        resolution: ApprovalResolution::Cancelled,
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit ApprovalResolved Cancelled");

        let result = ToolResultMessage {
            tool_call_id: tool_call.id.clone(),
            tool_name: tool_call.name.clone(),
            content: Vec::new(),
            details: serde_json::json!({"error": "approval_cancelled"}),
            is_error: true,
            timestamp: test_timestamp(),
        };
        let result_for_turn = result.clone();
        let result_id = "cancelled-result".to_owned();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageStart {
                        message_id: result_id.clone(),
                        message: Box::new(PublicMessage::ToolResult(result.clone())),
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("commit tool result MessageStart");

        let (result_barrier, _) = MessageCommitBarrier::channel();
        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::MessageEnd {
                        message_id: result_id,
                        message: Box::new(PublicMessage::ToolResult(result)),
                    },
                    commit_barrier: None,
                    message_commit_barrier: Some(result_barrier),
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: Some(tool_call.id.clone()),
                },
            )
            .await
            .expect("commit tool result MessageEnd")
            .resolve_message_receipts();

        assert!(
            bridge.approval_prepared_tools.is_empty(),
            "approval_prepared_tools must be released after cancelled ToolResult"
        );

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding: binding.clone(),
                    event: AgentEvent::TurnEnd {
                        message: Some(Box::new(assistant_message.clone())),
                        tool_results: vec![result_for_turn],
                    },
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("close cancelled turn");

        bridge
            .commit(
                &writer,
                RunOutput {
                    binding,
                    event: AgentEvent::AgentEnd,
                    commit_barrier: None,
                    message_commit_barrier: None,
                    retry_wait_commit_barrier: None,
                    approval_command: None,
                    approval_not_started: None,
                    approval_cancelled: None,
                },
            )
            .await
            .expect("AgentEnd after cancelled approval");
        assert_eq!(bridge.phase, RunPhase::Finished);

        let (state, error_code, start_count) = sqlx::query_as::<_, (String, Option<String>, i64)>(
            "SELECT state, error_code,
                (SELECT COUNT(*) FROM agent_events WHERE event_type='tool_execution_start'
                 AND json_extract(envelope, '$.tool_call_id')=?)
             FROM tool_executions WHERE tool_call_id=?",
        )
        .bind(&tool_call.id)
        .bind(&tool_call.id)
        .fetch_one(store.pool())
        .await
        .expect("read cancelled tool execution");
        assert_eq!(state, "cancelled");
        assert_eq!(error_code.as_deref(), Some("approval_cancelled"));
        assert_eq!(
            start_count, 0,
            "cancelled approval must not emit ToolExecutionStart"
        );
    }
}
