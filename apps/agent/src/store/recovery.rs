use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::agent::{AgentEvent, ApprovalResolution};
use crate::gateway::Command;
use crate::memory::{HydratedMemoryRuntime, estimate::ProviderContextItemWithFootprint};
use crate::provider::types::{
    ContextMessage, Message, PublicAssistantContent, PublicMessage, StopReason, ToolCall,
    ToolResultMessage, UserContent,
};
use crate::runtime::contracts::{GenerationRecoveryFence, ProcessGenerationLease};

use super::{
    ApplicationKind, ApplyReceiptOutcome, DataKeyPurpose, EventBatch, EventWrite, EventWriter,
    HydrationReceiptIdentity, PhysicalReapAttestation, PhysicalRecoveryIntent,
    PhysicalRecoveryIntentRequest, PhysicalRecoveryReceipt, Projection, RecoveryBatchWriter,
    RunPhase, Store, ToolExecutionMutation,
    crypto::decrypt_content,
    event_log::{EVENT_DIGEST_BYTES, EventChainEntry, extend_event_chain, verify_event_head},
    event_writer::DurableEventMetadata,
    tool_result_message_id, verify_command_payload_digest,
};

const EVENT_EVIDENCE_PAGE_ROWS: i64 = 64;
const PENDING_COMMAND_MAX_COUNT: usize = 32;
const PENDING_COMMAND_MAX_BYTES: usize = 4 * 1024 * 1024;
const RECOVERY_GROUP_MAX_COMMANDS: usize = 16;
const RECOVERY_GROUP_MAX_BYTES: usize = 1024 * 1024;
const PROCESS_RESTARTED_ERROR_CODE: &str = "process_restarted";
const PROCESS_RESTARTED_TOOL_RESULT: &str = "process restarted before tool execution";
const APPROVAL_CANCELLED_ERROR_CODE: &str = "approval_cancelled";
const APPROVAL_CANCELLED_TOOL_RESULT: &str =
    "approval was cancelled after process restart before tool execution";
const PHYSICAL_RECOVERY_INDETERMINATE_TOOL_RESULT: &str = "ツールの実行中にプロセスが停止したため、実行結果は不明です。処理が完了している可能性と、完了していない可能性があります。";

/// Typed identity for one durably pending approval whose prepared tool must be
/// closed during logical suffix recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingApprovalRecovery {
    pub request_id: String,
    pub tool_call_id: String,
}

/// Authenticated pre-disposition Error-context evidence carried to the T26
/// logical-resume consumer. Store hydration has already verified the
/// transcript row, exact anchor, encrypted provider-context items, and active
/// item key. T26 must choose the normal retry/overflow/terminal disposition,
/// build the fixed Invalidate through EventWriter, and fence resume until its
/// common application reaches `applied`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingErrorContextRecovery {
    pub message_id: String,
    pub message_seq: u64,
    pub item_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryStep {
    Reclassify {
        command_id: String,
    },
    ApplyControl {
        command_id: String,
    },
    EmitAgentStart {
        command_id: String,
        run_id: String,
    },
    EmitTurnStart {
        command_id: String,
        run_id: String,
        turn_id: String,
    },
    InjectStoredGroup {
        run_id: String,
        turn_id: String,
        application_kind: ApplicationKind,
        command_ids: Vec<String>,
    },
    EmitUserMessageEnd {
        command_id: String,
        run_id: String,
        turn_id: String,
    },
    StartAssistant {
        command_id: String,
        run_id: String,
        turn_id: String,
    },
    ResumeAssistantFromDurableEvents {
        command_id: String,
        run_id: String,
        turn_id: String,
        pending_error_context: Option<PendingErrorContextRecovery>,
    },
    /// T23/T26 restart seam for an assistant turn that crashed while a real
    /// ApprovalBroker request was durably pending. T26 must consume this before
    /// resuming the assistant: atomically close the approval/prepared tool as
    /// Cancelled, then continue any separately planned stored steer group.
    CancelPendingApproval {
        command_id: String,
        run_id: String,
        turn_id: String,
        request_id: String,
        tool_call_id: String,
    },
    ResumeHardSteerFromDurableEvents {
        command_id: String,
        run_id: String,
        turn_id: String,
    },
    ResumeCancellationFromDurableEvents {
        command_id: String,
        run_id: String,
        turn_id: String,
        /// Present when Abort already committed `cancel_requested` but the
        /// worker crashed before emitting the approval cancellation/result.
        ///
        /// T26 must consume this as one cancellation suffix transaction:
        /// `ApprovalResolved(Cancelled)`, the prepared tool terminal/result,
        /// and the Abort owner close must all commit together. This packet does
        /// not claim that the T26 consumer exists.
        pending_approval: Option<PendingApprovalRecovery>,
    },
}

/// Store-owned consumer for authenticated completed-assistant restart seams.
///
/// This executor intentionally supports only a complete single-step assistant
/// suffix: either `ResumeAssistantFromDurableEvents` or
/// `CancelPendingApproval`. Every other logical-recovery shape remains NotReady
/// until its own canonical consumer is implemented. The supported paths never
/// call a provider or tool: terminal calls reuse their exact durable result,
/// rowless calls receive a synthetic pre-execution error, and a typed pending
/// approval atomically becomes a cancelled prepared tool plus its error result.
/// An Error terminal without tool calls closes the interrupted run while keeping
/// its exact error in the transcript. This does not resume the old process's
/// remaining automatic retries or turn the failed attempt into a success.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LogicalRecoveryExecutor;

#[derive(Clone)]
struct MissingToolResult {
    call: ToolCall,
    message_id: String,
    result: ToolResultMessage,
}

struct CancelledPendingApproval {
    request_id: String,
    call: ToolCall,
    message_id: String,
    result: ToolResultMessage,
}

enum MissingToolDisposition {
    ProcessRestarted(MissingToolResult),
    ApprovalCancelled(CancelledPendingApproval),
}

/// A `running` tool execution that the physical recovery batch currently under
/// construction is about to terminate as `indeterminate`, together with the
/// ToolResult that batch writes for it.
///
/// The logical suffix has to be planned against these pending resolutions
/// rather than against the database, because both halves commit in the same
/// transaction and therefore neither can observe the other through durable
/// rows.
#[derive(Clone)]
struct PendingPhysicalResolution {
    message_id: String,
    result: ToolResultMessage,
}

struct AssistantRecoverySnapshot {
    command_id: String,
    command_seq: u64,
    run_id: String,
    turn_id: String,
    assistant: PublicMessage,
    tool_results: Vec<ToolResultMessage>,
    missing_dispositions: Vec<MissingToolDisposition>,
}

impl LogicalRecoveryExecutor {
    pub(crate) async fn execute(
        &self,
        store: &Store,
        steps: &[RecoveryStep],
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
    ) -> Result<()> {
        // The guard authenticates the current lifecycle checkpoint and keeps
        // this recovery writer exclusive through the final atomic batch.
        let writer = EventWriter::new(Arc::new(store.clone()));
        let mut recovery = writer.begin_bootstrap_recovery(lease, fence).await?;
        let batch =
            Self::plan_batch(store, &recovery, steps, lease.generation(), &HashMap::new()).await?;
        recovery
            .apply_recovery_batch(batch)
            .await
            .context("failed to atomically close assistant restart seam")?;
        Ok(())
    }

    /// Build the logical suffix batch without committing it.
    ///
    /// `pending_physical` names the `running` executions whose
    /// `indeterminate` terminals and ToolResults are already staged in the
    /// caller's batch. Boot physical recovery supplies them so that the ledger,
    /// the terminals, and this suffix reach SQLite as one transaction.
    async fn plan_batch(
        store: &Store,
        recovery: &super::event_writer::BootstrapRecoveryGuard<'_>,
        steps: &[RecoveryStep],
        executor_generation: crate::runtime::contracts::ProcessGeneration,
        pending_physical: &HashMap<String, PendingPhysicalResolution>,
    ) -> Result<EventBatch> {
        let [step] = steps else {
            bail!(
                "Store LogicalRecoveryExecutor only supports one assistant logical-recovery step; received {} ordered step(s)",
                steps.len()
            );
        };
        let (command_id, run_id, expected_owner_turn_id, expected_pending, planned_active_turn_id) =
            match step {
                RecoveryStep::ResumeAssistantFromDurableEvents {
                    command_id,
                    run_id,
                    turn_id,
                    pending_error_context,
                } => {
                    if pending_error_context.is_some() {
                        bail!(
                            "Store LogicalRecoveryExecutor does not support an undisposed Error provider context"
                        );
                    }
                    (command_id, run_id, Some(turn_id.as_str()), None, None)
                }
                RecoveryStep::CancelPendingApproval {
                    command_id,
                    run_id,
                    turn_id,
                    request_id,
                    tool_call_id,
                } => (
                    command_id,
                    run_id,
                    None,
                    Some(PendingApprovalRecovery {
                        request_id: request_id.clone(),
                        tool_call_id: tool_call_id.clone(),
                    }),
                    Some(turn_id.as_str()),
                ),
                _ => bail!(
                    "Store LogicalRecoveryExecutor does not support this assistant logical-recovery step"
                ),
            };

        let active_turn_id = recovery.authenticated_open_turn(run_id)?.to_owned();
        if planned_active_turn_id.is_some_and(|planned| planned != active_turn_id) {
            bail!("pending approval recovery turn does not match the authenticated open turn");
        }
        let mut transaction = store
            .pool()
            .begin()
            .await
            .context("failed to begin logical-recovery snapshot")?;
        super::event_writer::authenticate_event_log_snapshot(store, &mut transaction)
            .await
            .context("failed to authenticate logical-recovery event snapshot")?;
        let messages = store
            .hydrate_messages(&mut transaction)
            .await
            .context("failed to authenticate logical-recovery transcript")?;
        let snapshot = AssistantRecoverySnapshot::load(
            &mut transaction,
            &messages,
            command_id,
            run_id,
            expected_owner_turn_id,
            &active_turn_id,
            expected_pending.as_ref(),
            pending_physical,
        )
        .await?;
        transaction
            .commit()
            .await
            .context("failed to commit logical-recovery inspection")?;

        snapshot.into_batch(executor_generation)
    }
}

impl AssistantRecoverySnapshot {
    #[expect(
        clippy::too_many_arguments,
        reason = "logical recovery authenticates each owner, turn, approval, and physical-resolution dimension explicitly"
    )]
    async fn load(
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        messages: &[ContextMessage],
        command_id: &str,
        run_id: &str,
        expected_owner_turn_id: Option<&str>,
        active_turn_id: &str,
        expected_pending: Option<&PendingApprovalRecovery>,
        pending_physical: &HashMap<String, PendingPhysicalResolution>,
    ) -> Result<Self> {
        let command = sqlx::query(
            "SELECT seq, command_kind, status, application_kind, run_id, turn_id, run_phase
             FROM inbound_commands WHERE command_id = ? LIMIT 1",
        )
        .bind(command_id)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to inspect logical-recovery command owner")?
        .ok_or_else(|| anyhow::anyhow!("logical-recovery command {command_id} disappeared"))?;
        let command_seq = u64::try_from(command.try_get::<i64, _>("seq")?)
            .context("logical-recovery command sequence is negative")?;
        let command_kind: String = command.try_get("command_kind")?;
        let status: String = command.try_get("status")?;
        let application_kind: Option<String> = command.try_get("application_kind")?;
        let stored_run_id: Option<String> = command.try_get("run_id")?;
        let stored_turn_id: Option<String> = command.try_get("turn_id")?;
        let run_phase: String = command.try_get("run_phase")?;
        let stored_owner_turn_id = stored_turn_id.as_deref().ok_or_else(|| {
            anyhow::anyhow!("logical-recovery command {command_id} has no durable owner turn")
        })?;
        let application_kind = application_kind.as_deref().ok_or_else(|| {
            anyhow::anyhow!("logical-recovery command {command_id} has no application kind")
        })?;
        ApplicationKind::parse(application_kind).with_context(|| {
            format!("logical-recovery command {command_id} has an invalid application kind")
        })?;
        if command_kind != "user_message"
            || status != "applying"
            || stored_run_id.as_deref() != Some(run_id)
            || expected_owner_turn_id.is_some_and(|expected| stored_owner_turn_id != expected)
            || run_phase != "assistant_started"
        {
            bail!(
                "logical-recovery command {command_id} is not the exact live assistant_started owner"
            );
        }

        // Multiple assistant attempts may exist after retries. The latest
        // authenticated MessageEnd in the currently open turn is the suffix
        // owner. The command row's turn authenticates the original owner only;
        // provider/tool continuation may have advanced the run to another turn.
        let assistant_row = sqlx::query(
            "SELECT m.id, m.seq
             FROM messages AS m
             JOIN agent_events AS e ON e.seq = m.seq
             WHERE m.role = 'assistant'
               AND e.event_type = 'message_end'
               AND json_extract(e.internal_metadata, '$.run_id') = ?
               AND json_extract(e.internal_metadata, '$.turn_id') = ?
             ORDER BY m.seq DESC LIMIT 1",
        )
        .bind(run_id)
        .bind(active_turn_id)
        .fetch_optional(&mut **transaction)
        .await
        .context("failed to locate assistant logical-recovery owner")?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "logical-recovery turn {run_id}/{active_turn_id} has no authenticated assistant MessageEnd"
            )
        })?;
        let assistant_message_id: String = assistant_row.try_get("id")?;
        let assistant_seq = u64::try_from(assistant_row.try_get::<i64, _>("seq")?)
            .context("assistant logical-recovery sequence is negative")?;
        let assistant = messages
            .iter()
            .find_map(|message| match message {
                ContextMessage::Persisted {
                    id,
                    seq,
                    message: Message::Assistant(_),
                } if id == &assistant_message_id && *seq == assistant_seq => {
                    Some(crate::memory::overflow::context_message_to_public(message))
                }
                _ => None,
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "assistant logical-recovery owner {assistant_message_id}/{assistant_seq} is absent from the authenticated transcript"
                )
            })?;
        let PublicMessage::Assistant(assistant_message) = &assistant else {
            unreachable!("assistant transcript variant was matched")
        };
        if assistant_message.stop_reason == StopReason::Error
            && assistant_message.content.iter().all(|item| {
                matches!(
                    item,
                    PublicAssistantContent::Text { .. } | PublicAssistantContent::Thinking { .. }
                )
            })
        {
            // A completed provider failure can be followed by a process exit
            // before the retry wait or normal TurnEnd/AgentEnd finishes. Close
            // that exact interrupted attempt, without replaying an external
            // effect or erasing the original error. Pending provider context is
            // rejected by plan_batch until its disposition is implemented.
            let unsettled: i64 = sqlx::query_scalar(
                "SELECT
                    (SELECT COUNT(*) FROM tool_executions
                     WHERE run_id = ? AND state IN ('prepared', 'running')) +
                    (SELECT COUNT(*) FROM approval_log
                     WHERE run_id = ? AND state = 'pending') +
                    (SELECT COUNT(*) FROM agent_events
                     WHERE event_type = 'message_start' AND seq > ?
                       AND json_extract(internal_metadata, '$.run_id') = ?
                       AND json_extract(internal_metadata, '$.turn_id') = ?)",
            )
            .bind(run_id)
            .bind(run_id)
            .bind(i64::try_from(assistant_seq).context("assistant sequence overflows i64")?)
            .bind(run_id)
            .bind(active_turn_id)
            .fetch_one(&mut **transaction)
            .await
            .context("failed to inspect the interrupted Error suffix")?;
            if unsettled != 0 || expected_pending.is_some() || !pending_physical.is_empty() {
                bail!("Error logical recovery cannot close unresolved work or a later attempt");
            }
            return Ok(Self {
                command_id: command_id.to_owned(),
                command_seq,
                run_id: run_id.to_owned(),
                turn_id: active_turn_id.to_owned(),
                assistant,
                tool_results: Vec::new(),
                missing_dispositions: Vec::new(),
            });
        }
        if assistant_message.stop_reason != StopReason::ToolUse || assistant_message.interrupted {
            bail!(
                "assistant logical-recovery owner {assistant_message_id} is not a completed ToolUse MessageEnd"
            );
        }

        let mut tool_call_ids = HashSet::new();
        let mut calls = Vec::new();
        for item in &assistant_message.content {
            match item {
                PublicAssistantContent::ToolCall { tool_call, .. } => {
                    if !tool_call_ids.insert(tool_call.id.as_str()) {
                        bail!(
                            "assistant logical-recovery owner contains duplicate ToolCall {}",
                            tool_call.id
                        );
                    }
                    calls.push(tool_call.clone());
                }
                PublicAssistantContent::RejectedToolCall { .. } => {
                    bail!(
                        "Store LogicalRecoveryExecutor does not support mixed rejected ToolCall recovery"
                    );
                }
                PublicAssistantContent::Text { .. } | PublicAssistantContent::Thinking { .. } => {}
            }
        }
        if calls.is_empty() {
            bail!("ToolUse logical recovery requires at least one ToolCall");
        }

        let mut persisted_results = HashMap::<String, (String, u64, ToolResultMessage)>::new();
        for message in messages {
            let ContextMessage::Persisted {
                id,
                seq,
                message: Message::ToolResult(result),
            } = message
            else {
                continue;
            };
            if tool_call_ids.contains(result.tool_call_id.as_str())
                && persisted_results
                    .insert(
                        result.tool_call_id.clone(),
                        (id.clone(), *seq, result.clone()),
                    )
                    .is_some()
            {
                bail!(
                    "logical-recovery ToolCall {} has multiple durable results",
                    result.tool_call_id
                );
            }
        }

        let mut tool_results = Vec::with_capacity(calls.len());
        let mut missing_dispositions = Vec::new();
        let mut saw_cancelled_approval = false;
        let mut saw_unsettled_gap = false;
        for call in calls {
            let tool_row = sqlx::query(
                "SELECT command_id, run_id, state, error_code
                 FROM tool_executions WHERE tool_call_id = ? LIMIT 1",
            )
            .bind(&call.id)
            .fetch_optional(&mut **transaction)
            .await
            .with_context(|| format!("failed to inspect ToolCall {}", call.id))?;
            let approval_row = sqlx::query(
                "SELECT id, run_id, turn_id, state FROM approval_log
                 WHERE tool_call_id = ? LIMIT 1",
            )
            .bind(&call.id)
            .fetch_optional(&mut **transaction)
            .await
            .with_context(|| format!("failed to inspect ToolCall {} approval", call.id))?;
            if saw_unsettled_gap
                && (tool_row.is_some()
                    || approval_row.is_some()
                    || persisted_results.contains_key(call.id.as_str()))
            {
                bail!(
                    "ToolCall {} has durable execution evidence after an earlier rowless or pending call",
                    call.id
                );
            }
            if let Some(approval) = approval_row.as_ref() {
                let approval_run: String = approval.try_get("run_id")?;
                let approval_turn: String = approval.try_get("turn_id")?;
                if approval_run != run_id || approval_turn != active_turn_id {
                    bail!(
                        "ToolCall {} approval belongs to another run or turn",
                        call.id
                    );
                }
                let approval_state: String = approval.try_get("state")?;
                if approval_state == "pending" {
                    let approval_id: String = approval.try_get("id")?;
                    let Some(expected) = expected_pending else {
                        bail!(
                            "Store LogicalRecoveryExecutor does not support unplanned pending approval for ToolCall {}",
                            call.id
                        );
                    };
                    if approval_id != expected.request_id || call.id != expected.tool_call_id {
                        bail!("pending approval does not match the typed recovery step");
                    }
                    if saw_cancelled_approval {
                        bail!("assistant logical recovery contains multiple pending approvals");
                    }
                    let tool = tool_row.as_ref().ok_or_else(|| {
                        anyhow::anyhow!(
                            "pending approval ToolCall {} has no prepared tool execution",
                            call.id
                        )
                    })?;
                    let owner_command: String = tool.try_get("command_id")?;
                    let owner_run: String = tool.try_get("run_id")?;
                    let state: String = tool.try_get("state")?;
                    if owner_command != command_id || owner_run != run_id || state != "prepared" {
                        bail!(
                            "pending approval ToolCall {} is not the exact prepared durable owner",
                            call.id
                        );
                    }
                    if persisted_results.contains_key(call.id.as_str()) {
                        bail!(
                            "pending approval ToolCall {} already has a durable result",
                            call.id
                        );
                    }
                    let message_id =
                        tool_result_message_id(&assistant_message_id, call.id.as_str());
                    let result = ToolResultMessage {
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        content: vec![UserContent::Text {
                            text: APPROVAL_CANCELLED_TOOL_RESULT.to_owned(),
                        }],
                        details: json!({ "error": APPROVAL_CANCELLED_ERROR_CODE }),
                        is_error: true,
                        timestamp: Utc::now(),
                    };
                    tool_results.push(result.clone());
                    missing_dispositions.push(MissingToolDisposition::ApprovalCancelled(
                        CancelledPendingApproval {
                            request_id: approval_id,
                            call,
                            message_id,
                            result,
                        },
                    ));
                    saw_cancelled_approval = true;
                    saw_unsettled_gap = true;
                    continue;
                }
            }

            let expected_message_id =
                tool_result_message_id(&assistant_message_id, call.id.as_str());
            match tool_row {
                Some(row) => {
                    let owner_command: String = row.try_get("command_id")?;
                    let owner_run: String = row.try_get("run_id")?;
                    let state: String = row.try_get("state")?;
                    if owner_command != command_id || owner_run != run_id {
                        bail!("ToolCall {} belongs to another durable owner", call.id);
                    }
                    if state == "running"
                        && let Some(pending) = pending_physical.get(call.id.as_str())
                    {
                        // The caller's batch turns this execution into an
                        // `indeterminate` terminal plus its ToolResult in the
                        // same transaction that will commit this suffix, so the
                        // durable row cannot show the resolution yet. Plan
                        // against the staged result and check it exactly as the
                        // durable branch below checks a persisted one.
                        if pending.message_id != expected_message_id
                            || pending.result.tool_call_id != call.id
                            || pending.result.tool_name != call.name
                            || !pending.result.is_error
                        {
                            bail!(
                                "physically recovered ToolCall {} disagrees with its staged indeterminate ToolResult",
                                call.id
                            );
                        }
                        if persisted_results.contains_key(call.id.as_str()) {
                            bail!(
                                "physically recovered ToolCall {} already has a durable result",
                                call.id
                            );
                        }
                        tool_results.push(pending.result.clone());
                        continue;
                    }
                    if matches!(state.as_str(), "prepared" | "running") {
                        bail!(
                            "Store LogicalRecoveryExecutor does not support active ToolCall {} in state {state}",
                            call.id
                        );
                    }
                    if !matches!(
                        state.as_str(),
                        "succeeded" | "failed" | "cancelled" | "indeterminate" | "not_started"
                    ) {
                        bail!("ToolCall {} has unsupported durable state {state}", call.id);
                    }
                    let Some((message_id, result_seq, result)) =
                        persisted_results.remove(call.id.as_str())
                    else {
                        bail!(
                            "terminal ToolCall {} has no exact durable ToolResult MessageEnd",
                            call.id
                        );
                    };
                    if message_id != expected_message_id
                        || result_seq <= assistant_seq
                        || result.tool_call_id != call.id
                        || result.tool_name != call.name
                        || result.is_error != (state != "succeeded")
                    {
                        bail!(
                            "terminal ToolCall {} disagrees with its exact durable ToolResult",
                            call.id
                        );
                    }
                    tool_results.push(result);
                }
                None => {
                    if approval_row.is_some() {
                        bail!(
                            "rowless ToolCall {} has approval evidence and cannot be classified as pre-policy",
                            call.id
                        );
                    }
                    if persisted_results.contains_key(call.id.as_str()) {
                        bail!(
                            "rowless ToolCall {} already has a durable result without a terminal execution row",
                            call.id
                        );
                    }
                    let result = ToolResultMessage {
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        content: vec![UserContent::Text {
                            text: PROCESS_RESTARTED_TOOL_RESULT.to_owned(),
                        }],
                        details: json!({ "error": PROCESS_RESTARTED_TOOL_RESULT }),
                        is_error: true,
                        timestamp: Utc::now(),
                    };
                    tool_results.push(result.clone());
                    missing_dispositions.push(MissingToolDisposition::ProcessRestarted(
                        MissingToolResult {
                            call,
                            message_id: expected_message_id,
                            result,
                        },
                    ));
                    saw_unsettled_gap = true;
                }
            }
        }
        if !persisted_results.is_empty() {
            bail!("authenticated transcript contains unowned results for current ToolCalls");
        }
        if expected_pending.is_some() && !saw_cancelled_approval {
            bail!("typed pending approval recovery target is absent from the active turn");
        }

        Ok(Self {
            command_id: command_id.to_owned(),
            command_seq,
            run_id: run_id.to_owned(),
            turn_id: active_turn_id.to_owned(),
            assistant,
            tool_results,
            missing_dispositions,
        })
    }

    fn into_batch(
        self,
        executor_generation: crate::runtime::contracts::ProcessGeneration,
    ) -> Result<EventBatch> {
        let disposition_writes = self
            .missing_dispositions
            .iter()
            .map(|disposition| match disposition {
                MissingToolDisposition::ProcessRestarted(_) => 2usize,
                MissingToolDisposition::ApprovalCancelled(_) => 4usize,
            })
            .sum::<usize>();
        let mut writes = Vec::with_capacity(disposition_writes.saturating_add(2));
        for disposition in self.missing_dispositions {
            match disposition {
                MissingToolDisposition::ApprovalCancelled(cancelled) => {
                    writes.push(EventWrite {
                        event: Some(super::DurableEvent::approval_resolved(
                            cancelled.request_id.clone(),
                            ApprovalResolution::Cancelled,
                            "runtime".to_owned(),
                        )?),
                        projections: vec![Projection::Approval(super::ApprovalMutation::Resolve {
                            request_id: cancelled.request_id,
                            state: "cancelled",
                            actor: "runtime".to_owned(),
                        })],
                    });
                    writes.push(EventWrite {
                        event: Some(super::DurableEvent::tool_execution_end(
                            cancelled.call.id.clone(),
                            serde_json::to_value(&cancelled.result)?,
                            true,
                            "cancelled".to_owned(),
                            Some(APPROVAL_CANCELLED_ERROR_CODE.to_owned()),
                        )?),
                        projections: vec![Projection::ToolExecution(
                            super::ToolExecutionMutation::Finish {
                                tool_call_id: cancelled.call.id,
                                expected: "prepared",
                                state: "cancelled",
                                error_code: Some(APPROVAL_CANCELLED_ERROR_CODE),
                            },
                        )],
                    });
                    let message = PublicMessage::ToolResult(cancelled.result);
                    writes.push(EventWrite {
                        event: Some(super::DurableEvent::message(
                            "message_start",
                            &cancelled.message_id,
                            &message,
                        )?),
                        projections: Vec::new(),
                    });
                    writes.push(EventWrite {
                        event: Some(super::DurableEvent::message(
                            "message_end",
                            &cancelled.message_id,
                            &message,
                        )?),
                        projections: vec![Projection::MessageEnd {
                            message_id: cancelled.message_id,
                            role: "tool_result",
                            message,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    });
                }
                MissingToolDisposition::ProcessRestarted(missing) => {
                    let message = PublicMessage::ToolResult(missing.result);
                    writes.push(EventWrite {
                        event: Some(super::DurableEvent::message(
                            "message_start",
                            &missing.message_id,
                            &message,
                        )?),
                        projections: Vec::new(),
                    });
                    writes.push(EventWrite {
                        event: Some(super::DurableEvent::message(
                            "message_end",
                            &missing.message_id,
                            &message,
                        )?),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: missing.message_id,
                                role: "tool_result",
                                message,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            Projection::ToolExecution(super::ToolExecutionMutation::Skip {
                                tool_call_id: missing.call.id.clone(),
                                command_id: self.command_id.clone(),
                                run_id: self.run_id.clone(),
                                turn_id: self.turn_id.clone(),
                                executor_generation,
                                idempotency_key: format!("{}/{}", self.command_id, missing.call.id),
                                error_code: PROCESS_RESTARTED_ERROR_CODE,
                            }),
                        ],
                    });
                }
            }
        }
        writes.push(EventWrite {
            event: Some(super::DurableEvent::turn_end(
                &self.run_id,
                &self.turn_id,
                self.assistant,
                self.tool_results,
            )?),
            projections: Vec::new(),
        });
        writes.push(EventWrite {
            event: Some(super::DurableEvent::agent_end(&self.run_id)?),
            projections: vec![Projection::CommandApplied {
                command_id: self.command_id,
                command_seq: self.command_seq,
                run_id: Some(self.run_id),
            }],
        });
        Ok(EventBatch {
            writes,
            injected_commands: Vec::new(),
        })
    }
}

/// Authenticated cold-boot hydration boundary returned by T17.
///
/// T17 decrypts and validates persisted transcript anchors, provider context,
/// and Store-owned memory/command/phase state, then completes the logical
/// suffix plan. T26 consumes this typed boundary and composes the production
/// RunCore without T17 taking ownership of T19-T21 memory, T23 ApprovalBroker,
/// production ToolRegistry, or T26 composition.
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "T17 boundary payload consumed by T26 bootstrap composition"
)]
pub(crate) struct HydratedRunState {
    pub scope: super::AgentScope,
    pub lease: ProcessGenerationLease,
    pub fence: GenerationRecoveryFence,
    pub receipt: super::HydrationReceiptIdentity,
    pub messages: Vec<ContextMessage>,
    pub provider_context: Vec<ProviderContextItemWithFootprint>,
    /// Authenticated, ciphertext-free Store handoff. A future T26 consumer
    /// will pass this opaque value to `ThreeLayerMemory::from_hydrated`.
    pub memory: HydratedMemoryRuntime,
    pub resume: ResumeDirective,
    /// Still-unclassified inputs whose authenticated gateway replay must be
    /// admitted once by the new Session. They remain durably `received`.
    pub received_user_commands: Vec<ReceivedUserCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ReceivedUserCommand {
    pub command_id: String,
    pub seq: u64,
}

/// Runtime instruction carried only by a hydration result that reached a
/// durable fixed point. Any nonempty logical suffix is returned as the typed
/// `LogicalRecoveryRequired` outcome instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResumeDirective {
    AdmitCommands,
}

/// Result of a T17 hydration attempt.
///
/// Non-empty physical recovery intents keep the boot fail-closed until T27
/// supplies a valid `PhysicalRecoveryReceipt` and the logical suffix is
/// completed in one EventWriter transaction.
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum HydrationOutcome {
    PhysicalRecoveryRequired(Vec<super::PhysicalRecoveryIntentRequest>),
    LogicalRecoveryRequired {
        // T26 owns applying this suffix. Until then this variant deliberately
        // exposes no receipt or ready signal; only `Complete` does.
        #[allow(
            dead_code,
            reason = "T26 bootstrap composition will consume the ordered logical suffix; T17 must keep it typed and fail-closed until then"
        )]
        steps: Vec<RecoveryStep>,
    },
    #[allow(
        dead_code,
        reason = "T26 bootstrap composition is the first production consumer of the complete hydrated state and its ready receipt"
    )]
    Complete(HydratedRunState),
}

#[derive(Clone, Debug)]
struct PendingCommand {
    command_id: String,
    command_kind: String,
    application_kind: Option<ApplicationKind>,
    run_id: Option<String>,
    turn_id: Option<String>,
    phase: RunPhase,
}

pub(crate) struct SuffixRecovery;

impl SuffixRecovery {
    /// Consume an authenticated control-plane reap attestation for boot-only
    /// physical intents, then durably record the honest unknown outcome. The
    /// attestation proves only that execution cannot still be live; it cannot
    /// prove whether the external effect happened, so every terminal is
    /// `indeterminate`, never success or failure.
    pub(crate) async fn apply_boot_physical_receipt(
        store: &Store,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        reap_attestation: &PhysicalReapAttestation,
        requests: &[PhysicalRecoveryIntentRequest],
    ) -> Result<(ApplyReceiptOutcome, PhysicalRecoveryReceipt)> {
        let writer = EventWriter::new(Arc::new(store.clone()));
        let mut recovery = writer.begin_bootstrap_recovery(lease, fence).await?;
        let (receipt, batch) = Self::plan_boot_physical_receipt(
            store,
            &recovery,
            lease,
            fence,
            reap_attestation,
            requests,
        )
        .await?;
        let (outcome, _) = recovery
            .apply_physical_recovery_batch(lease, fence, receipt.clone(), batch)
            .await?;
        Ok((outcome, receipt))
    }

    /// Build the single transaction the receipt contract requires: the T17
    /// application ledger, every `running -> indeterminate` terminal with its
    /// ToolResult, and the whole logical suffix that resolving those intents
    /// makes necessary.
    ///
    /// The suffix is deliberately not left to the next `hydrate` iteration.
    /// That loop stays as an idempotent re-verification, but it must never be
    /// where state required for correctness is created: a crash between two
    /// commits would leave the ledger and the `indeterminate` terminals durable
    /// with the rest of the suffix missing, which is precisely the torn state
    /// `docs/agent/implementation-plan.md` forbids ("全件なし／全件あり").
    async fn plan_boot_physical_receipt(
        store: &Store,
        recovery: &super::event_writer::BootstrapRecoveryGuard<'_>,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        reap_attestation: &PhysicalReapAttestation,
        requests: &[PhysicalRecoveryIntentRequest],
    ) -> Result<(PhysicalRecoveryReceipt, EventBatch)> {
        if requests.is_empty() {
            bail!("boot physical recovery requires at least one intent");
        }

        // Boot is still NotReady, the epoch fence has removed all older
        // containers, and the caller holds the single writer gate through the
        // commit, so no producer can advance this Store between this head read
        // and the EventWriter transaction. EventWriter revalidates the exact
        // contiguous suffix and receipt binding before commit.
        let head: Option<i64> = sqlx::query_scalar("SELECT last_seq FROM event_log_heads")
            .fetch_optional(store.pool())
            .await
            .context("read event-log head for boot physical recovery")?;
        let first_seq = u64::try_from(head.unwrap_or(0))
            .context("event-log head is outside the physical recovery sequence range")?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("physical recovery first sequence overflow"))?;
        let physical_event_count = requests
            .len()
            .checked_mul(3)
            .ok_or_else(|| anyhow::anyhow!("physical recovery event count overflow"))?;

        let mut writes = Vec::with_capacity(physical_event_count);
        let mut intents = Vec::with_capacity(requests.len());
        let mut pending_physical = HashMap::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            let terminal_seq = first_seq
                .checked_add(
                    u64::try_from(index)
                        .context("physical recovery intent index exceeds u64")?
                        .checked_mul(3)
                        .ok_or_else(|| {
                            anyhow::anyhow!("physical recovery terminal sequence overflow")
                        })?,
                )
                .ok_or_else(|| anyhow::anyhow!("physical recovery terminal sequence overflow"))?;
            let result = ToolResultMessage {
                tool_call_id: request.tool_call_id.clone(),
                tool_name: request.tool_name.clone(),
                content: vec![UserContent::Text {
                    text: PHYSICAL_RECOVERY_INDETERMINATE_TOOL_RESULT.to_owned(),
                }],
                details: json!({
                    "error": "indeterminate",
                    "outcome": "indeterminate",
                    "message": PHYSICAL_RECOVERY_INDETERMINATE_TOOL_RESULT,
                }),
                is_error: true,
                timestamp: Utc::now(),
            };
            let message = PublicMessage::ToolResult(result.clone());
            let message_id =
                tool_result_message_id(&request.assistant_message_id, &request.tool_call_id);
            writes.extend([
                EventWrite {
                    event: Some(super::DurableEvent::tool_execution_end(
                        request.tool_call_id.clone(),
                        serde_json::to_value(&result)?,
                        true,
                        "indeterminate".to_owned(),
                        Some("indeterminate".to_owned()),
                    )?),
                    projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                        tool_call_id: request.tool_call_id.clone(),
                        expected: "running",
                        state: "indeterminate",
                        error_code: Some("indeterminate"),
                    })],
                },
                EventWrite {
                    event: Some(super::DurableEvent::message(
                        "message_start",
                        &message_id,
                        &message,
                    )?),
                    projections: Vec::new(),
                },
                EventWrite {
                    event: Some(super::DurableEvent::message(
                        "message_end",
                        &message_id,
                        &message,
                    )?),
                    projections: vec![Projection::MessageEnd {
                        message_id: message_id.clone(),
                        role: "tool_result",
                        message,
                        append_to_l0: true,
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
                    }],
                },
            ]);
            if pending_physical
                .insert(
                    request.tool_call_id.clone(),
                    PendingPhysicalResolution { message_id, result },
                )
                .is_some()
            {
                bail!(
                    "boot physical recovery received duplicate intents for ToolCall {}",
                    request.tool_call_id
                );
            }
            intents.push(PhysicalRecoveryIntent {
                tool_call_id: request.tool_call_id.clone(),
                command_id: request.command_id.clone(),
                run_id: request.run_id.clone(),
                executor_generation: request.executor_generation,
                indeterminate_terminal_seq: terminal_seq,
            });
        }

        // Plan the rest of the suffix against the state this batch is about to
        // create. `plan_one_command` classifies from the durable command phase
        // and event evidence, and the physical terminals change neither, so this
        // is the same plan the next hydration would produce - it just reaches
        // SQLite in the same transaction instead of a later one.
        let (steps, _) = Self::plan_boot_recovery(store, recovery).await?;
        if !steps.is_empty() {
            let suffix = LogicalRecoveryExecutor::plan_batch(
                store,
                recovery,
                &steps,
                lease.generation(),
                &pending_physical,
            )
            .await
            .context("plan the logical suffix this physical recovery makes necessary")?;
            if !suffix.injected_commands.is_empty() {
                bail!("physical recovery suffix must not inject commands");
            }
            writes.extend(suffix.writes);
        }

        let batch = EventBatch {
            writes,
            injected_commands: Vec::new(),
        };
        // EventWriter materializes one `command_disposition` event per terminal
        // command projection, so the range this receipt must name is longer than
        // the writes planned above.
        let event_count = super::event_writer::materialized_event_count(&batch);
        if event_count < physical_event_count {
            bail!("physical recovery batch lost a terminal event while planning its suffix");
        }
        let last_seq = first_seq
            .checked_add(
                u64::try_from(event_count - 1)
                    .context("physical recovery event count exceeds u64")?,
            )
            .ok_or_else(|| anyhow::anyhow!("physical recovery last sequence overflow"))?;

        let identity = HydrationReceiptIdentity {
            personality_agent_id: lease.personality_agent_id().clone(),
            lease_id: lease.lease_id().to_owned(),
            generation: lease.generation(),
            fence_id: fence.fence_id().to_owned(),
            intent_count: intents.len(),
        };
        let mut receipt = PhysicalRecoveryReceipt {
            receipt_id: format!("physical-recovery-{}", identity.stable_id()),
            lease: lease.clone(),
            fence: fence.clone(),
            reap_attestation: reap_attestation.clone(),
            intents,
            logical_suffix_first_seq: first_seq,
            logical_suffix_last_seq: last_seq,
            digest: String::new(),
        };
        receipt.digest = receipt.canonical_digest();

        Ok((receipt, batch))
    }

    /// Test-only seam for the crash-boundary fixtures: build exactly the batch
    /// `apply_boot_physical_receipt` would commit, and hand it back unapplied so
    /// the caller can drive it through an abrupt transaction failpoint.
    #[cfg(test)]
    pub(crate) async fn plan_boot_physical_receipt_for_test(
        store: &Store,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        reap_attestation: &PhysicalReapAttestation,
        requests: &[PhysicalRecoveryIntentRequest],
    ) -> Result<(PhysicalRecoveryReceipt, EventBatch)> {
        let writer = EventWriter::new(Arc::new(store.clone()));
        let recovery = writer.begin_bootstrap_recovery(lease, fence).await?;
        Self::plan_boot_physical_receipt(store, &recovery, lease, fence, reap_attestation, requests)
            .await
    }

    /// Complete a T17 hydration suffix after T27 has supplied an authenticated
    /// physical recovery receipt.  EventWriter owns the transaction boundary;
    /// this wrapper intentionally does not kill/reap processes or persist the
    /// T27 proof store.
    #[allow(
        dead_code,
        reason = "receipt replay is retained for exact boot-recovery tests"
    )]
    pub(crate) async fn apply_physical_receipt(
        writer: &EventWriter,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        receipt: PhysicalRecoveryReceipt,
        batch: EventBatch,
    ) -> Result<(ApplyReceiptOutcome, Vec<u64>)> {
        writer
            .apply_physical_recovery(lease, fence, receipt, batch)
            .await
    }

    /// Plans and persists only the restart prefix owned by T12.
    ///
    /// T15 uses this boundary only as a startup gate: after the T12-safe prefix
    /// work, it returns any remaining plan as `RecoveryRequired` instead of
    /// consuming it. T17 consumes the full suffix plan during hydration; T26
    /// composes that hydrated state into production.
    pub(crate) async fn recover_t12_prefix(
        store: &Store,
        writer: &EventWriter,
    ) -> Result<Vec<RecoveryStep>> {
        loop {
            let steps = Self::plan_next_without_history_scan(store, Some(writer)).await?;
            let Some(step) = steps.first() else {
                return Ok(steps);
            };
            let mut applied_abort = false;
            for step in &steps {
                let RecoveryStep::ApplyControl { command_id } = step else {
                    continue;
                };
                let Some(seq) = idle_abort_sequence(store, command_id).await? else {
                    continue;
                };
                writer.apply_idle_abort_cutoff(command_id, seq).await?;
                applied_abort = true;
                break;
            }
            if applied_abort {
                continue;
            }
            match step {
                RecoveryStep::Reclassify { command_id }
                    if can_classify_as_idle(store, command_id).await? =>
                {
                    writer
                        .apply(EventBatch {
                            writes: vec![EventWrite {
                                event: None,
                                projections: vec![Projection::CommandClassified {
                                    command_id: command_id.clone(),
                                    application_kind: ApplicationKind::IdleRun,
                                    run_id: Uuid::now_v7().to_string(),
                                    turn_id: Uuid::now_v7().to_string(),
                                }],
                            }],
                            injected_commands: Vec::new(),
                        })
                        .await?;
                }
                RecoveryStep::ApplyControl { command_id } => {
                    let Some(seq) = recoverable_noop_approval_sequence(store, command_id).await?
                    else {
                        return Ok(steps);
                    };
                    writer
                        .apply(EventBatch {
                            writes: vec![EventWrite {
                                event: None,
                                projections: vec![Projection::CommandApplied {
                                    command_id: command_id.clone(),
                                    command_seq: seq,
                                    run_id: None,
                                }],
                            }],
                            injected_commands: Vec::new(),
                        })
                        .await?;
                }
                _ => return Ok(steps),
            }
        }
    }

    /// Plans only the next missing durable action for the oldest pending
    /// command/group.
    #[allow(
        dead_code,
        reason = "T12 persists prefix work; T15 only gates and returns RecoveryRequired; T17 consumes the full suffix plan"
    )]
    pub(crate) async fn plan(store: &Store) -> Result<Vec<RecoveryStep>> {
        Self::plan_next_without_history_scan(store, None).await
    }

    /// Plans the complete ordered suffix for all pending commands.
    ///
    /// This is the T17-owned durable recovery boundary: it returns every
    /// remaining recovery step, including the runtime integration packets
    /// (`ResumeAssistantFromDurableEvents`, `ResumeHardSteerFromDurableEvents`,
    /// `ResumeCancellationFromDurableEvents`) that T16/T26 consume.  It does
    /// not itself execute runtime behavior.
    #[allow(
        dead_code,
        reason = "T17 hydration boundary consumed by T16/T26 runtime startup"
    )]
    pub(crate) async fn plan_full_suffix(
        store: &Store,
        recovery: &super::event_writer::BootstrapRecoveryGuard<'_>,
    ) -> Result<Vec<RecoveryStep>> {
        validate_pending_window(store).await?;
        let commands = all_pending_commands(store).await?;
        if commands.is_empty() {
            durable_event_evidence(store, EventEvidence::default()).await?;
            return Ok(Vec::new());
        }
        let mut events = EventEvidence::required_for(&commands)?;
        events = durable_event_evidence(store, events).await?;
        let mut steps = Vec::with_capacity(commands.len());
        let mut injected_groups = HashSet::new();
        for command in &commands {
            let mut step = plan_one_command(store, command, &events).await?;
            match &step {
                RecoveryStep::ResumeAssistantFromDurableEvents {
                    command_id,
                    run_id,
                    turn_id: _,
                    ..
                } => {
                    if let Some((pending_turn_id, pending)) =
                        pending_approval_for_recovery(store, recovery, run_id).await?
                    {
                        let active_turn_id = recovery.authenticated_open_turn(run_id)?.to_owned();
                        if pending_turn_id != active_turn_id {
                            bail!(
                                "run {run_id} has a pending approval outside its authenticated open turn {active_turn_id}"
                            );
                        }
                        step = RecoveryStep::CancelPendingApproval {
                            command_id: command_id.clone(),
                            run_id: run_id.clone(),
                            turn_id: active_turn_id,
                            request_id: pending.request_id,
                            tool_call_id: pending.tool_call_id,
                        };
                    }
                }
                RecoveryStep::ResumeCancellationFromDurableEvents {
                    command_id,
                    run_id,
                    turn_id,
                    ..
                } => {
                    let pending_approval =
                        pending_approval_for_recovery(store, recovery, run_id).await?;
                    if pending_approval
                        .as_ref()
                        .is_some_and(|(pending_turn_id, _)| pending_turn_id != turn_id)
                    {
                        bail!(
                            "run {run_id} cancellation turn {turn_id} does not own its authenticated pending approval"
                        );
                    }
                    step = RecoveryStep::ResumeCancellationFromDurableEvents {
                        command_id: command_id.clone(),
                        run_id: run_id.clone(),
                        turn_id: turn_id.clone(),
                        pending_approval: pending_approval.map(|(_, pending)| pending),
                    };
                }
                _ => {}
            }
            if let RecoveryStep::InjectStoredGroup {
                ref run_id,
                ref turn_id,
                application_kind,
                ref command_ids,
            } = step
            {
                if command_ids.is_empty() {
                    bail!("InjectStoredGroup for {run_id}/{turn_id} produced no command_ids");
                }
                let key = (
                    run_id.clone(),
                    turn_id.clone(),
                    application_kind,
                    command.phase,
                );
                if !injected_groups.insert(key) {
                    continue;
                }
            }
            steps.push(step);
        }
        Ok(steps)
    }

    /// Unclassified user inputs need the ordinary Session router, not a
    /// fabricated classification or terminal disposition during bootstrap.
    /// Keep them on disk while repairing the preceding run, then hand their
    /// exact identities to the Session after hydration reaches a fixed point.
    pub(crate) async fn plan_boot_recovery(
        store: &Store,
        recovery: &super::event_writer::BootstrapRecoveryGuard<'_>,
    ) -> Result<(Vec<RecoveryStep>, Vec<ReceivedUserCommand>)> {
        let steps = Self::plan_full_suffix(store, recovery).await?;
        let mut repairs = Vec::new();
        let mut received = Vec::new();
        for step in steps {
            if let RecoveryStep::Reclassify { command_id } = step {
                // plan_full_suffix authenticated the bounded pending window.
                // The bootstrap guard keeps this row stable through handoff.
                let seq: i64 = sqlx::query_scalar(
                    "SELECT seq FROM inbound_commands WHERE command_id=?
                     AND status='received' AND run_phase='received'
                     AND command_kind='user_message' AND application_kind IS NULL
                     AND run_id IS NULL AND turn_id IS NULL",
                )
                .bind(&command_id)
                .fetch_one(store.pool())
                .await
                .context("received user recovery plan does not name an unclassified input")?;
                received.push(ReceivedUserCommand {
                    command_id,
                    seq: u64::try_from(seq).context("received user sequence is negative")?,
                });
            } else {
                repairs.push(step);
            }
        }
        Ok((repairs, received))
    }

    async fn plan_next_without_history_scan(
        store: &Store,
        writer: Option<&EventWriter>,
    ) -> Result<Vec<RecoveryStep>> {
        validate_pending_window(store).await?;
        let Some(command) = next_pending_command(store).await? else {
            if let Some(writer) = writer {
                writer.initialize_recovery_checkpoint().await?;
            } else {
                durable_event_evidence(store, EventEvidence::default()).await?;
            }
            return Ok(Vec::new());
        };
        let mut events = EventEvidence::required_for(std::slice::from_ref(&command))?;
        if let Some(writer) = writer {
            writer.initialize_recovery_checkpoint().await?;
            for predicate in events.required.clone() {
                if writer
                    .has_recovery_lifecycle_evidence(
                        predicate.event_type,
                        &predicate.run_id,
                        predicate.turn_id.as_deref(),
                    )
                    .await?
                {
                    events.found.insert(predicate);
                }
            }
        } else {
            events = durable_event_evidence(store, events).await?;
        }
        Ok(vec![plan_one_command(store, &command, &events).await?])
    }
}

async fn pending_approval_for_recovery(
    store: &Store,
    recovery: &super::event_writer::BootstrapRecoveryGuard<'_>,
    run_id: &str,
) -> Result<Option<(String, PendingApprovalRecovery)>> {
    let authenticated = recovery.authenticated_pending_approval_for_run(run_id)?;
    let rows = sqlx::query(
        "SELECT a.id, a.tool_call_id, a.turn_id, t.state AS tool_state,
                t.run_id AS tool_run_id
         FROM approval_log a
         LEFT JOIN tool_executions t ON t.tool_call_id = a.tool_call_id
         WHERE a.run_id = ? AND a.state = 'pending'
         ORDER BY a.created_at, a.id",
    )
    .bind(run_id)
    .fetch_all(store.pool())
    .await
    .context("failed to inspect pending approval recovery state")?;
    if rows.len() > 1 {
        bail!(
            "run {run_id} has multiple projected pending approvals despite sequential one-at-a-time execution"
        );
    }
    let projected = if let Some(row) = rows.first() {
        let request_id: String = row.try_get("id")?;
        let tool_call_id: String = row.try_get("tool_call_id")?;
        let turn_id: String = row.try_get("turn_id")?;
        let tool_state: Option<String> = row.try_get("tool_state")?;
        let tool_run_id: Option<String> = row.try_get("tool_run_id")?;
        if tool_state.as_deref() != Some("prepared") || tool_run_id.as_deref() != Some(run_id) {
            bail!(
                "pending approval {request_id} recovery requires its exact prepared tool {tool_call_id} in run {run_id}"
            );
        }
        Some((request_id, tool_call_id, turn_id))
    } else {
        None
    };

    match (authenticated, projected) {
        (None, None) => Ok(None),
        (Some((request_id, tool_call_id, turn_id)), Some(projected))
            if projected == (request_id.clone(), tool_call_id.clone(), turn_id.clone()) =>
        {
            Ok(Some((
                turn_id,
                PendingApprovalRecovery {
                    request_id,
                    tool_call_id,
                },
            )))
        }
        (Some((request_id, tool_call_id, turn_id)), None) => bail!(
            "authenticated pending approval {request_id}/{tool_call_id} for {run_id}/{turn_id} is missing its exact projections"
        ),
        (None, Some((request_id, tool_call_id, turn_id))) => bail!(
            "projected pending approval {request_id}/{tool_call_id} for {run_id}/{turn_id} has no authenticated lifecycle owner"
        ),
        (Some(authenticated), Some(projected)) => bail!(
            "authenticated pending approval {authenticated:?} disagrees with projected pending approval {projected:?}"
        ),
    }
}

async fn plan_one_command(
    store: &Store,
    command: &PendingCommand,
    events: &EventEvidence,
) -> Result<RecoveryStep> {
    match command.phase {
        RunPhase::Received => {
            if command.command_kind == "user_message" {
                Ok(RecoveryStep::Reclassify {
                    command_id: command.command_id.clone(),
                })
            } else {
                Ok(RecoveryStep::ApplyControl {
                    command_id: command.command_id.clone(),
                })
            }
        }
        RunPhase::Classified => {
            let kind = required_kind(command)?;
            let run_id = required(command.run_id.as_deref(), "run_id", command)?;
            let turn_id = required(command.turn_id.as_deref(), "turn_id", command)?;
            if kind == ApplicationKind::IdleRun {
                Ok(RecoveryStep::EmitAgentStart {
                    command_id: command.command_id.clone(),
                    run_id: run_id.to_owned(),
                })
            } else {
                Ok(RecoveryStep::InjectStoredGroup {
                    run_id: run_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    application_kind: kind,
                    command_ids: load_bounded_group(store, run_id, turn_id, kind, command.phase)
                        .await?,
                })
            }
        }
        RunPhase::RunStarted => {
            let run_id = required(command.run_id.as_deref(), "run_id", command)?;
            if !events.has("agent_start", Some(run_id), None) {
                bail!(
                    "run_started command {} has no durable AgentStart evidence",
                    command.command_id
                );
            }
            Ok(RecoveryStep::EmitTurnStart {
                command_id: command.command_id.clone(),
                run_id: run_id.to_owned(),
                turn_id: required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
            })
        }
        RunPhase::TurnStarted => {
            let kind = required_kind(command)?;
            let run_id = required(command.run_id.as_deref(), "run_id", command)?;
            let turn_id = required(command.turn_id.as_deref(), "turn_id", command)?;
            if kind != ApplicationKind::RetrySteer
                && !events.has("turn_start", Some(run_id), Some(turn_id))
            {
                bail!(
                    "turn_started command {} has no durable TurnStart evidence",
                    command.command_id
                );
            }
            Ok(RecoveryStep::InjectStoredGroup {
                run_id: run_id.to_owned(),
                turn_id: turn_id.to_owned(),
                application_kind: kind,
                command_ids: load_bounded_group(store, run_id, turn_id, kind, command.phase)
                    .await?,
            })
        }
        RunPhase::UserStarted => Ok(RecoveryStep::EmitUserMessageEnd {
            command_id: command.command_id.clone(),
            run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
            turn_id: required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
        }),
        RunPhase::UserCommitted => Ok(RecoveryStep::StartAssistant {
            command_id: command.command_id.clone(),
            run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
            turn_id: required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
        }),
        RunPhase::AssistantStarted => Ok(RecoveryStep::ResumeAssistantFromDurableEvents {
            command_id: command.command_id.clone(),
            run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
            turn_id: required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
            pending_error_context: None,
        }),
        RunPhase::HardSteerRequested => Ok(RecoveryStep::ResumeHardSteerFromDurableEvents {
            command_id: command.command_id.clone(),
            run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
            turn_id: required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
        }),
        RunPhase::CancelRequested => Ok(RecoveryStep::ResumeCancellationFromDurableEvents {
            command_id: command.command_id.clone(),
            run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
            turn_id: required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
            pending_approval: None,
        }),
        RunPhase::Finished => {
            bail!(
                "finished command {} must have terminal status",
                command.command_id
            );
        }
    }
}

async fn next_pending_command(store: &Store) -> Result<Option<PendingCommand>> {
    // Abort is the only command allowed to preempt an earlier nonterminal
    // action. Its EventBatch closes the bounded earlier cutoff in seq order.
    let row = sqlx::query(
        "SELECT seq, command_id, command_kind, application_kind, run_id, turn_id, run_phase
         FROM inbound_commands
         WHERE status IN ('received','applying') AND command_kind='abort'
         ORDER BY seq LIMIT 1",
    )
    .fetch_optional(store.pool())
    .await
    .context("failed to find earliest pending Abort")?;
    let row = match row {
        Some(row) => Some(row),
        None => sqlx::query(
            "SELECT seq, command_id, command_kind, application_kind, run_id, turn_id, run_phase
                 FROM inbound_commands
                 WHERE status IN ('received','applying')
                 ORDER BY seq LIMIT 1",
        )
        .fetch_optional(store.pool())
        .await
        .context("failed to find oldest pending recovery action")?,
    };
    row.map(|row| pending_command_from_row(&row)).transpose()
}

fn pending_command_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<PendingCommand> {
    Ok(PendingCommand {
        command_id: row.try_get("command_id")?,
        command_kind: row.try_get("command_kind")?,
        application_kind: row
            .try_get::<Option<String>, _>("application_kind")?
            .map(|value| ApplicationKind::parse(&value))
            .transpose()?,
        run_id: row.try_get("run_id")?,
        turn_id: row.try_get("turn_id")?,
        phase: RunPhase::parse(row.try_get("run_phase")?)?,
    })
}

#[allow(
    dead_code,
    reason = "consumed by plan_full_suffix; kept separate for testability"
)]
async fn all_pending_commands(store: &Store) -> Result<Vec<PendingCommand>> {
    let rows = sqlx::query(
        "SELECT seq, command_id, command_kind, application_kind, run_id, turn_id, run_phase
         FROM inbound_commands
         WHERE status IN ('received','applying')
         ORDER BY (command_kind = 'abort') DESC, seq ASC",
    )
    .fetch_all(store.pool())
    .await
    .context("failed to load pending commands for suffix recovery")?;
    rows.iter().map(pending_command_from_row).collect()
}

async fn validate_pending_window(store: &Store) -> Result<()> {
    let mut after_seq = -1_i64;
    let mut ordinary_count = 0_usize;
    let mut ordinary_plaintext_bytes = 0_usize;
    let mut abort_count = 0_usize;
    loop {
        let row = sqlx::query(
            "SELECT seq, command_id, command_kind, payload_key_ref,
                    payload_ciphertext, payload_hmac
             FROM inbound_commands
             WHERE status IN ('received','applying') AND seq>?
             ORDER BY seq LIMIT 1",
        )
        .bind(after_seq)
        .fetch_optional(store.pool())
        .await
        .context("failed to scan bounded nonterminal command window")?;
        let Some(row) = row else {
            return Ok(());
        };
        let seq: i64 = row.try_get("seq")?;
        let plaintext = authenticated_pending_payload(store, &row).await?;
        let command_kind: String = row.try_get("command_kind")?;
        validate_stored_command_variant(command_kind.as_str(), &plaintext)?;
        if command_kind == "abort" {
            abort_count = abort_count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("pending Abort count overflow"))?;
            if abort_count > 1 {
                bail!("durable nonterminal command window contains more than one pending Abort");
            }
        } else {
            ordinary_count = ordinary_count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("pending ordinary command count overflow"))?;
            if ordinary_count > PENDING_COMMAND_MAX_COUNT {
                bail!(
                    "durable nonterminal ordinary command window exceeds {PENDING_COMMAND_MAX_COUNT} commands"
                );
            }
            ordinary_plaintext_bytes = ordinary_plaintext_bytes
                .checked_add(plaintext.len())
                .ok_or_else(|| {
                    anyhow::anyhow!("pending ordinary command plaintext byte count overflow")
                })?;
            if ordinary_plaintext_bytes > PENDING_COMMAND_MAX_BYTES {
                bail!(
                    "durable nonterminal ordinary command window exceeds {PENDING_COMMAND_MAX_BYTES} canonical bytes"
                );
            }
        }
        after_seq = seq;
    }
}

async fn load_bounded_group(
    store: &Store,
    run_id: &str,
    turn_id: &str,
    application_kind: ApplicationKind,
    expected_phase: RunPhase,
) -> Result<Vec<String>> {
    let mut after_seq = -1_i64;
    let mut command_ids = Vec::new();
    let mut plaintext_bytes = 0_usize;
    loop {
        let row = sqlx::query(
            "SELECT seq, command_id, command_kind, payload_key_ref,
                    payload_ciphertext, payload_hmac, run_phase
             FROM inbound_commands
             WHERE status='applying' AND run_id=? AND turn_id=? AND application_kind=?
               AND seq>?
             ORDER BY seq LIMIT 1",
        )
        .bind(run_id)
        .bind(turn_id)
        .bind(application_kind.as_str())
        .bind(after_seq)
        .fetch_optional(store.pool())
        .await
        .context("failed to scan bounded durable steer group")?;
        let Some(row) = row else {
            break;
        };
        if command_ids.len() == RECOVERY_GROUP_MAX_COMMANDS {
            bail!("durable steer group exceeds {RECOVERY_GROUP_MAX_COMMANDS} commands");
        }
        let phase = RunPhase::parse(row.try_get("run_phase")?)?;
        if phase != expected_phase {
            bail!(
                "durable steer group contains mixed phases {} and {}",
                expected_phase.as_str(),
                phase.as_str()
            );
        }
        let plaintext = authenticated_pending_payload(store, &row).await?;
        plaintext_bytes = plaintext_bytes
            .checked_add(plaintext.len())
            .ok_or_else(|| anyhow::anyhow!("steer group plaintext byte count overflow"))?;
        if plaintext_bytes > RECOVERY_GROUP_MAX_BYTES {
            bail!(
                "durable steer group exceeds {RECOVERY_GROUP_MAX_BYTES} canonical plaintext bytes"
            );
        }
        if row.try_get::<String, _>("command_kind")? != "user_message" {
            bail!("durable steer group contains a non-user command");
        }
        validate_stored_command_variant("user_message", &plaintext)?;
        after_seq = row.try_get("seq")?;
        command_ids.push(row.try_get("command_id")?);
    }
    if command_ids.is_empty() {
        bail!("durable recovery group is empty");
    }
    Ok(command_ids)
}

async fn authenticated_pending_payload(
    store: &Store,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Zeroizing<Vec<u8>>> {
    let seq = u64::try_from(row.try_get::<i64, _>("seq")?)
        .context("pending command sequence is outside u64")?;
    let key_ref: String = row.try_get("payload_key_ref")?;
    let key = store
        .data_key_by_ref(&key_ref)
        .await
        .with_context(|| format!("pending command {seq} key is unavailable"))?;
    if key.purpose != DataKeyPurpose::Command {
        bail!("pending command {seq} references a non-command data key");
    }
    let ciphertext: Vec<u8> = row.try_get("payload_ciphertext")?;
    let aad = store
        .scope()
        .row_aad("inbound_commands", seq.to_string(), DataKeyPurpose::Command);
    let plaintext = Zeroizing::new(
        decrypt_content(&key, &ciphertext, &aad)
            .with_context(|| format!("pending command {seq} failed authenticated recovery"))?,
    );
    verify_command_payload_digest(
        &key,
        &plaintext,
        row.try_get::<Vec<u8>, _>("payload_hmac")?.as_slice(),
    )
    .with_context(|| format!("pending command {seq} HMAC is invalid"))?;
    Ok(plaintext)
}

fn validate_stored_command_variant(command_kind: &str, plaintext: &[u8]) -> Result<()> {
    let command: Command =
        serde_json::from_slice(plaintext).context("stored pending command payload is invalid")?;
    let actual_kind = match command {
        Command::UserMessage { .. } => "user_message",
        Command::Abort {} => "abort",
        Command::ApprovalDecision { .. } => "approval_decision",
    };
    if actual_kind != command_kind {
        bail!("stored pending command kind does not match authenticated payload");
    }
    Ok(())
}

async fn can_classify_as_idle(store: &Store, command_id: &str) -> Result<bool> {
    let row = sqlx::query(
        "SELECT seq FROM inbound_commands
         WHERE command_id=? AND command_kind='user_message'
           AND status='received' AND run_phase='received'",
    )
    .bind(command_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let seq: i64 = row.try_get("seq")?;
    let earlier_pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inbound_commands
         WHERE seq < ? AND status IN ('received','applying')",
    )
    .bind(seq)
    .fetch_one(store.pool())
    .await?;
    let active_or_starting: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inbound_commands
         WHERE command_kind='user_message' AND status='applying'",
    )
    .fetch_one(store.pool())
    .await?;
    Ok(earlier_pending == 0 && active_or_starting == 0)
}

async fn idle_abort_sequence(store: &Store, command_id: &str) -> Result<Option<u64>> {
    let row = sqlx::query(
        "SELECT seq FROM inbound_commands
         WHERE command_id=? AND command_kind='abort'
           AND status='received' AND run_phase='received'",
    )
    .bind(command_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let owner_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inbound_commands
         WHERE command_kind='user_message' AND status='applying'
           AND run_phase IN (
             'user_started','user_committed','assistant_started',
             'hard_steer_requested','cancel_requested'
           )",
    )
    .fetch_one(store.pool())
    .await?;
    if owner_count != 0 {
        return Ok(None);
    }
    let seq: i64 = row.try_get("seq")?;
    u64::try_from(seq)
        .context("stored Abort sequence is outside u64")
        .map(Some)
}

async fn recoverable_noop_approval_sequence(
    store: &Store,
    command_id: &str,
) -> Result<Option<u64>> {
    let row = sqlx::query(
        "SELECT seq, payload_key_ref, payload_ciphertext, payload_hmac
         FROM inbound_commands
         WHERE command_id=? AND command_kind='approval_decision'
           AND status='received' AND run_phase='received'",
    )
    .bind(command_id)
    .fetch_optional(store.pool())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let seq: i64 = row.try_get("seq")?;
    let seq = u64::try_from(seq).context("stored ApprovalDecision sequence is outside u64")?;
    let key_ref: String = row.try_get("payload_key_ref")?;
    let key = store
        .data_key_by_ref(&key_ref)
        .await
        .context("stored ApprovalDecision key is unavailable")?;
    if key.purpose != DataKeyPurpose::Command {
        bail!("stored ApprovalDecision references a non-command data key");
    }
    let aad = store
        .scope()
        .row_aad("inbound_commands", seq.to_string(), DataKeyPurpose::Command);
    let ciphertext: Vec<u8> = row.try_get("payload_ciphertext")?;
    let plaintext = Zeroizing::new(
        decrypt_content(&key, &ciphertext, &aad)
            .context("stored ApprovalDecision failed authenticated recovery")?,
    );
    let payload_hmac: Vec<u8> = row.try_get("payload_hmac")?;
    verify_command_payload_digest(&key, &plaintext, &payload_hmac)
        .context("stored ApprovalDecision HMAC is invalid")?;
    let command: Command =
        serde_json::from_slice(&plaintext).context("stored ApprovalDecision payload is invalid")?;
    let Command::ApprovalDecision { request_id, .. } = command else {
        bail!("approval_decision row contains a different command variant");
    };
    let state: Option<String> = sqlx::query_scalar("SELECT state FROM approval_log WHERE id=?")
        .bind(request_id)
        .fetch_optional(store.pool())
        .await?;
    if state.as_deref() == Some("pending") {
        return Ok(None);
    }
    Ok(Some(seq))
}

fn required_kind(command: &PendingCommand) -> Result<ApplicationKind> {
    command.application_kind.ok_or_else(|| {
        anyhow::anyhow!(
            "pending command {} phase {} has no application_kind",
            command.command_id,
            command.phase.as_str()
        )
    })
}

fn required<'a>(value: Option<&'a str>, field: &str, command: &PendingCommand) -> Result<&'a str> {
    value.ok_or_else(|| {
        anyhow::anyhow!(
            "pending command {} phase {} has no {field}",
            command.command_id,
            command.phase.as_str()
        )
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EvidencePredicate {
    event_type: &'static str,
    run_id: String,
    turn_id: Option<String>,
}

#[derive(Default)]
struct EventEvidence {
    required: HashSet<EvidencePredicate>,
    found: HashSet<EvidencePredicate>,
}

impl EventEvidence {
    fn required_for(commands: &[PendingCommand]) -> Result<Self> {
        let mut predicates = HashSet::new();
        for command in commands {
            match command.phase {
                RunPhase::RunStarted => {
                    predicates.insert(EvidencePredicate {
                        event_type: "agent_start",
                        run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
                        turn_id: None,
                    });
                }
                RunPhase::TurnStarted if required_kind(command)? != ApplicationKind::RetrySteer => {
                    predicates.insert(EvidencePredicate {
                        event_type: "turn_start",
                        run_id: required(command.run_id.as_deref(), "run_id", command)?.to_owned(),
                        turn_id: Some(
                            required(command.turn_id.as_deref(), "turn_id", command)?.to_owned(),
                        ),
                    });
                }
                _ => {}
            }
        }
        Ok(Self {
            required: predicates,
            found: HashSet::new(),
        })
    }

    fn observe(&mut self, event: &AgentEvent, metadata: &DurableEventMetadata) {
        let Some(event_type) = event.durable_kind() else {
            return;
        };
        let Some(predicate) = self.required.iter().find(|predicate| {
            predicate.event_type == event_type
                && metadata.run_id.as_deref() == Some(predicate.run_id.as_str())
                && predicate
                    .turn_id
                    .as_deref()
                    .is_none_or(|turn_id| metadata.turn_id.as_deref() == Some(turn_id))
        }) else {
            return;
        };
        self.found.insert(predicate.clone());
    }

    fn has(&self, event_type: &str, run_id: Option<&str>, turn_id: Option<&str>) -> bool {
        self.found.iter().any(|predicate| {
            predicate.event_type == event_type
                && run_id.is_none_or(|run_id| predicate.run_id == run_id)
                && turn_id.is_none_or(|turn_id| predicate.turn_id.as_deref() == Some(turn_id))
        })
    }
}

async fn durable_event_evidence(
    store: &Store,
    mut evidence: EventEvidence,
) -> Result<EventEvidence> {
    let mut transaction = store.pool().begin().await?;
    let head_row = sqlx::query(
        "SELECT last_seq, event_count, chain_digest, key_ref, head_hmac
         FROM event_log_heads WHERE personality_agent_id=?",
    )
    .bind(store.scope().personality_agent_id.as_str())
    .fetch_optional(&mut *transaction)
    .await
    .context("failed to read authenticated event-log head")?;
    let authenticated_head = if let Some(row) = head_row {
        let last_seq = u64::try_from(row.try_get::<i64, _>("last_seq")?)
            .context("event-log head last sequence is outside u64")?;
        let event_count = u64::try_from(row.try_get::<i64, _>("event_count")?)
            .context("event-log head event count is outside u64")?;
        let key_ref: String = row.try_get("key_ref")?;
        let key = store
            .data_key_by_ref_in_transaction(&mut transaction, &key_ref)
            .await
            .context("event-log head key is unavailable")?;
        let chain_digest = verify_event_head(
            store.scope(),
            &key,
            last_seq,
            event_count,
            row.try_get::<Vec<u8>, _>("chain_digest")?.as_slice(),
            row.try_get::<Vec<u8>, _>("head_hmac")?.as_slice(),
        )
        .context("event-log head failed authenticated recovery")?;
        Some((last_seq, event_count, chain_digest, key_ref))
    } else {
        None
    };

    let mut after_seq = -1_i64;
    let mut expected_seq = 1_u64;
    let mut observed_count = 0_u64;
    let mut chain_digest = [0_u8; EVENT_DIGEST_BYTES];
    loop {
        // Page only fixed-size sequence metadata. Large event BLOBs are fetched
        // one row at a time below, so recovery never retains 64 batch-sized
        // ciphertexts at once.
        let page: Vec<i64> = sqlx::query_scalar(
            "SELECT seq FROM agent_events
             WHERE seq > ?
             ORDER BY seq
             LIMIT ?",
        )
        .bind(after_seq)
        .bind(EVENT_EVIDENCE_PAGE_ROWS)
        .fetch_all(&mut *transaction)
        .await
        .context("failed to read durable event sequence page")?;
        if page.is_empty() {
            break;
        }
        for page_seq in page {
            let page_seq =
                u64::try_from(page_seq).context("durable event page sequence is outside u64")?;
            if page_seq != expected_seq {
                bail!("durable event sequence gap: expected {expected_seq}, found {page_seq}");
            }
            let row = sqlx::query(
                "SELECT rowid AS physical_row_id, seq, event_type, internal_metadata,
                        raw_key_ref, raw_ciphertext, envelope, redaction_version
                 FROM agent_events WHERE seq=? LIMIT 1",
            )
            .bind(i64::try_from(page_seq).context("durable event sequence exceeds SQLite")?)
            .fetch_optional(&mut *transaction)
            .await
            .context("failed to read authenticated durable event evidence row")?
            .ok_or_else(|| {
                anyhow::anyhow!("durable event {page_seq} disappeared during recovery")
            })?;
            let physical_row_id: i64 = row.try_get("physical_row_id")?;
            let seq: i64 = row.try_get("seq")?;
            if physical_row_id != seq || seq < 0 || seq <= after_seq {
                bail!("durable event physical identity does not match its sequence");
            }
            let redaction_version: i64 = row.try_get("redaction_version")?;
            if redaction_version != i64::from(store.redactor().version()) {
                bail!("durable event uses an unsupported redaction version");
            }
            let key_ref: String = row.try_get("raw_key_ref")?;
            let key = store
                .data_key_by_ref_in_transaction(&mut transaction, &key_ref)
                .await
                .with_context(|| format!("durable event {seq} key is unavailable"))?;
            if key.purpose != DataKeyPurpose::Event {
                bail!("durable event {seq} references a non-event data key");
            }
            let aad = store
                .scope()
                .row_aad("agent_events", seq.to_string(), DataKeyPurpose::Event);
            let ciphertext: Vec<u8> = row.try_get("raw_ciphertext")?;
            let raw =
                Zeroizing::new(decrypt_content(&key, &ciphertext, &aad).with_context(|| {
                    format!("durable event {seq} failed authenticated recovery")
                })?);
            let regenerated = store
                .redactor()
                .redact_serialized(&raw)
                .with_context(|| format!("durable event {seq} raw event is invalid"))?;
            let envelope: String = row.try_get("envelope")?;
            if regenerated != envelope {
                bail!(
                    "durable event {seq} redacted projection does not match authenticated raw event"
                );
            }
            let event: AgentEvent = serde_json::from_slice(&raw)
                .with_context(|| format!("durable event {seq} is outside the closed T12 schema"))?;
            let event_type: String = row.try_get("event_type")?;
            if event.durable_kind() != Some(event_type.as_str())
                || matches!(
                    event,
                    AgentEvent::MessageUpdate { .. }
                        | AgentEvent::ToolExecutionUpdate { .. }
                        | AgentEvent::Error { .. }
                )
            {
                bail!("durable event {seq} public type and internal event_type disagree");
            }
            let internal_metadata: String = row.try_get("internal_metadata")?;
            let metadata: DurableEventMetadata = serde_json::from_str(&internal_metadata)
                .with_context(|| format!("durable event {seq} internal metadata is invalid"))?;
            evidence.observe(&event, &metadata);
            chain_digest = extend_event_chain(
                &chain_digest,
                EventChainEntry {
                    seq: page_seq,
                    event_type: &event_type,
                    internal_metadata: &internal_metadata,
                    key_ref: &key_ref,
                    ciphertext: &ciphertext,
                    envelope: &envelope,
                    redaction_version: u32::try_from(redaction_version)
                        .context("durable event redaction version is outside u32")?,
                },
            );
            observed_count = observed_count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("durable event count overflow"))?;
            expected_seq = expected_seq
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("durable event sequence overflow"))?;
            after_seq = seq;
        }
    }
    match authenticated_head {
        None if observed_count == 0 => {}
        None => bail!("durable events exist without an authenticated event-log head"),
        Some((last_seq, event_count, expected_digest, key_ref)) => {
            let observed_last = expected_seq - 1;
            if observed_last != last_seq
                || observed_count != event_count
                || chain_digest != expected_digest
            {
                bail!("durable event history does not match authenticated head for key {key_ref}");
            }
        }
    }
    transaction.commit().await?;
    Ok(evidence)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use anyhow::{Result, bail};
    use serde_json::json;

    use super::*;
    use crate::{
        agent::{ApprovalRequest, ReviewProjection},
        gateway::{
            ApprovalDecision, Command, CommandEnvelope, DeferredApprovalRule, InboundCommand,
        },
        provider::types::{
            ApiProtocol, ProviderOrigin, PublicAssistantMessage, Usage, UserMessage,
        },
        runtime::contracts::{DirectChatProvenanceV1, PersonalityAgentId, ProcessGeneration},
        store::{
            AgentScope, DurableEvent, EventBatch, EventWrite, EventWriter, InjectedCommand,
            Projection, ToolExecutionMutation,
            crypto::{DATA_KEY_BYTES, WrappingKey},
            event_writer::ApprovalMutation,
            user_message_id,
        },
    };

    const TOOL_USE_RECOVERY_COMMAND_ID: &str = "00000000-0000-4000-8000-000000000091";
    const TOOL_USE_RECOVERY_RUN_ID: &str = "run-tool-use-recovery";
    const TOOL_USE_RECOVERY_TURN_ID: &str = "turn-tool-use-recovery";
    const TOOL_USE_RECOVERY_CONTINUATION_TURN_ID: &str = "turn-tool-use-recovery-continuation";
    const TOOL_USE_RECOVERY_INITIAL_ASSISTANT_ID: &str = "assistant-tool-use-recovery-initial";
    const TOOL_USE_RECOVERY_ASSISTANT_ID: &str = "assistant-tool-use-recovery";
    const TOOL_USE_RECOVERY_APPROVAL_ID: &str = "approval-tool-use-recovery";

    fn test_personality_agent_id() -> PersonalityAgentId {
        "0198f0f4-9b72-7000-8000-000000000001"
            .parse()
            .expect("canonical test PAID")
    }

    fn test_provenance() -> DirectChatProvenanceV1 {
        DirectChatProvenanceV1::new("tenant-test", test_personality_agent_id(), "human-test")
            .expect("valid direct-chat provenance")
    }

    struct TestKeyProvider(WrappingKey);

    #[async_trait::async_trait]
    impl super::super::KeyProvider for TestKeyProvider {
        async fn current_key(&self) -> Result<WrappingKey> {
            Ok(self.0.clone())
        }

        async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
            if key_id != self.0.key_id() {
                bail!("unknown key");
            }
            Ok(self.0.clone())
        }
    }

    async fn setup() -> (Arc<Store>, EventWriter) {
        let store: Arc<Store> = Store::in_memory(
            AgentScope {
                personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
            },
            Arc::new(TestKeyProvider(WrappingKey::new(
                "test",
                [0x61; DATA_KEY_BYTES],
            ))),
        )
        .await
        .expect("store")
        .into();
        let writer = EventWriter::new(store.clone());
        (store, writer)
    }

    fn test_generation() -> ProcessGeneration {
        ProcessGeneration::from_wire(7).expect("test generation")
    }

    fn tool_use_recovery_assistant() -> PublicMessage {
        tool_use_recovery_assistant_with_calls(&[
            ("tool-terminal-success", "read_file", 0_u32),
            ("tool-terminal-failure", "write_file", 1_u32),
            ("tool-rowless-messaging", "messaging", 2_u32),
        ])
    }

    fn tool_use_recovery_assistant_with_calls(calls: &[(&str, &str, u32)]) -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: calls
                .iter()
                .map(
                    |(id, name, wire_item_index)| PublicAssistantContent::ToolCall {
                        tool_call: ToolCall {
                            id: (*id).to_owned(),
                            name: (*name).to_owned(),
                            arguments: serde_json::from_value(json!({ "slot": *wire_item_index }))
                                .expect("object tool arguments"),
                            route: crate::provider::types::ToolInvocationRoute::Normal,
                        },
                        wire_item_index: *wire_item_index,
                    },
                )
                .collect(),
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "test-provider-instance".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "test-model".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        })
    }

    fn tool_use_recovery_initial_assistant() -> PublicMessage {
        PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![PublicAssistantContent::Text {
                text: "continuing with tools".to_owned(),
                wire_item_index: 0,
            }],
            model: "test-model".to_owned(),
            provider: "test-provider".to_owned(),
            origin: ProviderOrigin {
                provider_instance_id: "test-provider-instance".to_owned(),
                protocol: ApiProtocol::OpenAiChatCompletions,
                model: "test-model".to_owned(),
            },
            usage: Usage::default(),
            stop_reason: StopReason::Stop,
            error_message: None,
            provider_code: None,
            interrupted: false,
            timestamp: Utc::now(),
        })
    }

    async fn persist_running_tool(
        writer: &EventWriter,
        tool_call_id: &str,
        tool_name: &str,
        slot: u32,
    ) {
        let generation = test_generation();
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: None,
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Prepare {
                                tool_call_id: tool_call_id.to_owned(),
                                command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                                run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                                executor_generation: generation,
                                idempotency_key: format!(
                                    "{TOOL_USE_RECOVERY_COMMAND_ID}/{tool_call_id}"
                                ),
                            },
                        )],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::tool_execution_start(
                                tool_call_id.to_owned(),
                                tool_name.to_owned(),
                                json!({ "slot": slot }),
                                TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                                TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                                generation,
                            )
                            .expect("ToolExecutionStart"),
                        ),
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Start {
                                tool_call_id: tool_call_id.to_owned(),
                                run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                            },
                        )],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("prepare and start ToolCall fixture");
    }

    async fn persist_terminal_tool(
        writer: &EventWriter,
        tool_call_id: &str,
        tool_name: &str,
        slot: u32,
        is_error: bool,
    ) {
        persist_running_tool(writer, tool_call_id, tool_name, slot).await;

        let result = ToolResultMessage {
            tool_call_id: tool_call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            content: vec![UserContent::Text {
                text: if is_error {
                    "fixture failure".to_owned()
                } else {
                    "fixture success".to_owned()
                },
            }],
            details: json!({ "fixture": true, "is_error": is_error }),
            is_error,
            timestamp: Utc::now(),
        };
        let message = PublicMessage::ToolResult(result.clone());
        let message_id = tool_result_message_id(TOOL_USE_RECOVERY_ASSISTANT_ID, tool_call_id);
        let state = if is_error { "failed" } else { "succeeded" };
        let error_code = is_error.then_some("executor_failed");
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::tool_execution_end(
                                tool_call_id.to_owned(),
                                serde_json::to_value(&result).expect("tool result value"),
                                is_error,
                                state.to_owned(),
                                error_code.map(str::to_owned),
                            )
                            .expect("ToolExecutionEnd"),
                        ),
                        projections: vec![Projection::ToolExecution(
                            ToolExecutionMutation::Finish {
                                tool_call_id: tool_call_id.to_owned(),
                                expected: "running",
                                state,
                                error_code,
                            },
                        )],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", &message_id, &message)
                                .expect("tool result MessageStart"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", &message_id, &message)
                                .expect("tool result MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id,
                            role: "tool_result",
                            message,
                            append_to_l0: true,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("finish terminal ToolCall fixture");
    }

    async fn seed_tool_use_restart_seam(writer: &EventWriter, continuation_turn: bool) {
        seed_tool_use_restart_seam_with_assistant(
            writer,
            continuation_turn,
            tool_use_recovery_assistant(),
            &[
                ("tool-terminal-success", "read_file", 0, false),
                ("tool-terminal-failure", "write_file", 1, true),
            ],
        )
        .await;
    }

    async fn seed_tool_use_restart_seam_with_assistant(
        writer: &EventWriter,
        continuation_turn: bool,
        assistant: PublicMessage,
        terminal_tools: &[(&str, &str, u32, bool)],
    ) {
        let append_to_l0 = !matches!(
            &assistant,
            PublicMessage::Assistant(message) if message.stop_reason == StopReason::Error
        );
        persist_user(writer, 1, TOOL_USE_RECOVERY_COMMAND_ID).await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                        turn_id: TOOL_USE_RECOVERY_TURN_ID.to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify ToolUse recovery command");

        let command_id = crate::gateway::CommandId::parse(TOOL_USE_RECOVERY_COMMAND_ID)
            .expect("ToolUse recovery command ID");
        let message_id = user_message_id(&test_personality_agent_id(), &command_id);
        let received_at: String =
            sqlx::query_scalar("SELECT received_at FROM inbound_commands WHERE command_id=?")
                .bind(TOOL_USE_RECOVERY_COMMAND_ID)
                .fetch_one(writer.store().pool())
                .await
                .expect("ToolUse recovery command timestamp");
        let user = PublicMessage::User(UserMessage {
            content: vec![UserContent::Text {
                text: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
            }],
            timestamp: chrono::DateTime::parse_from_rfc3339(&received_at)
                .expect("stored command timestamp")
                .with_timezone(&Utc),
        });
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::agent_start(TOOL_USE_RECOVERY_RUN_ID)
                                .expect("AgentStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                            run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                            expected: RunPhase::Classified,
                            next: RunPhase::RunStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::turn_start(
                                TOOL_USE_RECOVERY_RUN_ID,
                                TOOL_USE_RECOVERY_TURN_ID,
                            )
                            .expect("TurnStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                            run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                            expected: RunPhase::RunStarted,
                            next: RunPhase::TurnStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_start", &message_id, &user)
                                .expect("user MessageStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                            run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                            expected: RunPhase::TurnStarted,
                            next: RunPhase::UserStarted,
                        }],
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message("message_end", &message_id, &user)
                                .expect("user MessageEnd"),
                        ),
                        projections: vec![
                            Projection::MessageEnd {
                                message_id: message_id.clone(),
                                role: "user",
                                message: user,
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            },
                            Projection::RunPhase {
                                command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                                run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                                expected: RunPhase::UserStarted,
                                next: RunPhase::UserCommitted,
                            },
                        ],
                    },
                ],
                injected_commands: vec![InjectedCommand::new(1, command_id, test_provenance())],
            })
            .await
            .expect("persist ToolUse recovery user turn");

        if continuation_turn {
            let initial_assistant = tool_use_recovery_initial_assistant();
            writer
                .apply(EventBatch {
                    writes: vec![
                        EventWrite {
                            event: Some(
                                DurableEvent::message_in_turn(
                                    "message_start",
                                    TOOL_USE_RECOVERY_INITIAL_ASSISTANT_ID,
                                    &initial_assistant,
                                    Some(TOOL_USE_RECOVERY_RUN_ID.to_owned()),
                                    Some(TOOL_USE_RECOVERY_TURN_ID.to_owned()),
                                )
                                .expect("initial assistant MessageStart"),
                            ),
                            projections: vec![Projection::RunPhase {
                                command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                                run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                                expected: RunPhase::UserCommitted,
                                next: RunPhase::AssistantStarted,
                            }],
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::message_in_turn(
                                    "message_end",
                                    TOOL_USE_RECOVERY_INITIAL_ASSISTANT_ID,
                                    &initial_assistant,
                                    Some(TOOL_USE_RECOVERY_RUN_ID.to_owned()),
                                    Some(TOOL_USE_RECOVERY_TURN_ID.to_owned()),
                                )
                                .expect("initial assistant MessageEnd"),
                            ),
                            projections: vec![Projection::MessageEnd {
                                message_id: TOOL_USE_RECOVERY_INITIAL_ASSISTANT_ID.to_owned(),
                                role: "assistant",
                                message: initial_assistant.clone(),
                                append_to_l0: true,
                                provider_context: Vec::new(),
                                eviction_footprint_tokens: 0,
                            }],
                        },
                        EventWrite {
                            event: Some(
                                DurableEvent::turn_end(
                                    TOOL_USE_RECOVERY_RUN_ID,
                                    TOOL_USE_RECOVERY_TURN_ID,
                                    initial_assistant,
                                    Vec::new(),
                                )
                                .expect("initial TurnEnd"),
                            ),
                            projections: Vec::new(),
                        },
                    ],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("persist initial assistant turn");

            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::turn_start(
                                TOOL_USE_RECOVERY_RUN_ID,
                                TOOL_USE_RECOVERY_CONTINUATION_TURN_ID,
                            )
                            .expect("continuation TurnStart"),
                        ),
                        projections: Vec::new(),
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("open continuation turn");
        }

        let active_turn_id = if continuation_turn {
            TOOL_USE_RECOVERY_CONTINUATION_TURN_ID
        } else {
            TOOL_USE_RECOVERY_TURN_ID
        };
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                TOOL_USE_RECOVERY_ASSISTANT_ID,
                                &assistant,
                                Some(TOOL_USE_RECOVERY_RUN_ID.to_owned()),
                                Some(active_turn_id.to_owned()),
                            )
                            .expect("ToolUse assistant MessageStart"),
                        ),
                        projections: if continuation_turn {
                            Vec::new()
                        } else {
                            vec![Projection::RunPhase {
                                command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                                run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                                expected: RunPhase::UserCommitted,
                                next: RunPhase::AssistantStarted,
                            }]
                        },
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_end",
                                TOOL_USE_RECOVERY_ASSISTANT_ID,
                                &assistant,
                                Some(TOOL_USE_RECOVERY_RUN_ID.to_owned()),
                                Some(active_turn_id.to_owned()),
                            )
                            .expect("ToolUse assistant MessageEnd"),
                        ),
                        projections: vec![Projection::MessageEnd {
                            message_id: TOOL_USE_RECOVERY_ASSISTANT_ID.to_owned(),
                            role: "assistant",
                            message: assistant,
                            append_to_l0,
                            provider_context: Vec::new(),
                            eviction_footprint_tokens: 0,
                        }],
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist ToolUse restart seam");

        for (tool_call_id, tool_name, slot, is_error) in terminal_tools {
            persist_terminal_tool(writer, tool_call_id, tool_name, *slot, *is_error).await;
        }
    }

    /// Replacement-epoch authority for the boot fixture: the fixture's running
    /// executions belong to generation 7, so generation 8 is the first epoch
    /// whose attestation can cover them.
    fn boot_recovery_authority(
        store: &Store,
    ) -> (
        ProcessGenerationLease,
        GenerationRecoveryFence,
        PhysicalReapAttestation,
    ) {
        let personality_agent_id = store.scope().personality_agent_id.clone();
        let lease = ProcessGenerationLease::new(
            personality_agent_id.clone(),
            ProcessGeneration::from_wire(8).expect("boot epoch generation"),
            "boot-physical-recovery-lease",
        )
        .expect("boot physical recovery lease");
        let fence = GenerationRecoveryFence::new(&lease, "boot-physical-recovery-fence")
            .expect("boot physical recovery fence");
        let attestation = PhysicalReapAttestation::from_wire(
            personality_agent_id.as_str(),
            8,
            "boot-physical-recovery-nonce".to_owned(),
            7,
        )
        .expect("boot physical reap attestation");
        (lease, fence, attestation)
    }

    async fn seed_boot_running_tools(writer: &EventWriter, calls: &[(&str, &str, u32)]) {
        seed_tool_use_restart_seam_with_assistant(
            writer,
            false,
            tool_use_recovery_assistant_with_calls(calls),
            &[],
        )
        .await;
        for (tool_call_id, tool_name, slot) in calls {
            persist_running_tool(writer, tool_call_id, tool_name, *slot).await;
        }
    }

    pub(crate) async fn setup_boot_running_tools(
        calls: &[(&str, &str, u32)],
    ) -> (Arc<Store>, EventWriter) {
        let (store, writer) = setup().await;
        seed_boot_running_tools(&writer, calls).await;
        (store, writer)
    }

    /// The same fixture on disk, so a test can drop the Store and reopen it the
    /// way a restarted process would.
    pub(crate) async fn setup_boot_running_tools_on_disk(
        database_path: &std::path::Path,
        calls: &[(&str, &str, u32)],
    ) -> (Arc<Store>, EventWriter) {
        let store = open_boot_running_tools_store(database_path).await;
        let writer = EventWriter::new(store.clone());
        seed_boot_running_tools(&writer, calls).await;
        (store, writer)
    }

    #[tokio::test]
    async fn physical_recovery_closes_owner_and_preserves_queued_received_inputs() {
        let (store, writer) =
            setup_boot_running_tools(&[("tool-queued-physical", "read_file", 0)]).await;
        for seq in [2, 3] {
            persist_user(&writer, seq, &format!("00000000-0000-4000-8000-{seq:012}")).await;
        }
        let (lease, fence, attestation) = boot_recovery_authority(&store);
        let HydrationOutcome::PhysicalRecoveryRequired(intents) = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate running tool with queued inputs")
        else {
            panic!("old physical execution needs recovery");
        };
        SuffixRecovery::apply_boot_physical_receipt(&store, &lease, &fence, &attestation, &intents)
            .await
            .expect("physical terminal and owner suffix must commit together despite the queue");
        assert_indeterminate_surface(&store, "tool-queued-physical").await;
        let HydrationOutcome::Complete(hydrated) = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate physically recovered owner")
        else {
            panic!("untouched queued inputs must not leave startup blocked");
        };
        assert_eq!(
            hydrated
                .received_user_commands
                .iter()
                .map(|command| command.seq)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(sqlx::query_as::<_, (String, i64, i64, i64)>(
            "SELECT status,
                (SELECT COUNT(*) FROM inbound_commands WHERE status='received' AND run_phase='received'),
                (SELECT COUNT(*) FROM agent_events WHERE event_type='agent_end'),
                (SELECT COUNT(*) FROM physical_recovery_receipt_applications)
             FROM inbound_commands WHERE seq=1",
        ).fetch_one(store.pool()).await.expect("atomic receipt and preserved queue"),
        ("applied".to_owned(), 2, 1, 1));
    }

    /// Admitting the co-committed suffix widened what may sit inside a
    /// receipt's sequence range, so the widening has to stay bound to the
    /// receipt's own owner. A receipt that claims a different owning command no
    /// longer authenticates the suffix's other ToolCalls, and is refused before
    /// anything commits.
    #[tokio::test]
    async fn a_receipt_range_admits_the_co_committed_suffix_only_for_its_own_owner() {
        let (store, writer) = setup_boot_running_tools_with_rowless_tail(
            &[
                ("tool-boot-owned", "read_file", 0),
                ("tool-boot-rowless", "write_file", 1),
            ],
            1,
        )
        .await;
        let (lease, fence, attestation) = boot_recovery_authority(&store);
        let intents = match store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate the owner-binding fixture")
        {
            HydrationOutcome::PhysicalRecoveryRequired(intents) => intents,
            other => panic!("fixture must require physical recovery: {other:?}"),
        };
        let (mut receipt, batch) = SuffixRecovery::plan_boot_physical_receipt_for_test(
            &store,
            &lease,
            &fence,
            &attestation,
            &intents,
        )
        .await
        .expect("plan the boot physical recovery batch");

        // Same events, same range - only the claimed owning command changes.
        for intent in &mut receipt.intents {
            intent.command_id = "00000000-0000-4000-8000-0000000000ff".to_owned();
        }
        receipt.digest = receipt.canonical_digest();

        let error = SuffixRecovery::apply_physical_receipt(&writer, &lease, &fence, receipt, batch)
            .await
            .expect_err("a receipt must not authenticate another owner's suffix");
        assert!(
            format!("{error:#}")
                .contains("recovery suffix contains a result for an unrelated tool"),
            "{error:#}"
        );
        let (ledger, state): (i64, String) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM physical_recovery_receipt_applications),
                (SELECT state FROM tool_executions WHERE tool_call_id = 'tool-boot-owned')",
        )
        .fetch_one(store.pool())
        .await
        .expect("read the refused state");
        assert_eq!((ledger, state.as_str()), (0, "running"));
    }

    #[cfg(unix)]
    #[tokio::test]
    #[ignore = "subprocess entry point for the boot physical-recovery suffix atomicity test"]
    async fn boot_physical_receipt_suffix_child() {
        let boundary = std::env::var("SUMI_T27_BOUNDARY").expect("child boundary environment");
        let database_path = std::path::PathBuf::from(
            std::env::var("SUMI_T27_DATABASE").expect("child database environment"),
        );
        let readiness_path = std::path::PathBuf::from(
            std::env::var("SUMI_T27_READY").expect("child readiness environment"),
        );
        let (store, writer) =
            setup_boot_running_tools_on_disk(&database_path, &[("tool-boot-kill", "read_file", 0)])
                .await;
        let (lease, fence, attestation) = boot_recovery_authority(&store);
        let intents = match store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate the hard-kill fixture")
        {
            HydrationOutcome::PhysicalRecoveryRequired(intents) => intents,
            other => panic!("hard-kill fixture must require physical recovery: {other:?}"),
        };
        let (receipt, batch) = SuffixRecovery::plan_boot_physical_receipt_for_test(
            &store,
            &lease,
            &fence,
            &attestation,
            &intents,
        )
        .await
        .expect("plan the boot physical recovery batch");
        writer
            .apply_boot_physical_recovery_with_abrupt_failpoint(
                &lease,
                &fence,
                &receipt,
                batch,
                "boot_physical_receipt_suffix",
                boundary == "after_commit",
                &readiness_path,
            )
            .await
            .expect("abrupt failpoint must not return");
        panic!("abrupt failpoint returned");
    }

    /// The receipt transaction now carries the ledger, the `indeterminate`
    /// terminal and the whole logical suffix, so a hard kill on either side of
    /// its commit must leave all of them or none of them.
    #[cfg(unix)]
    #[tokio::test]
    async fn boot_physical_receipt_and_its_suffix_are_all_or_none_across_a_hard_kill() {
        for boundary in ["before_commit", "after_commit"] {
            let root = std::env::temp_dir().join(format!(
                "sumi-t27-boot-suffix-{boundary}-{}",
                Uuid::now_v7()
            ));
            std::fs::create_dir_all(&root).expect("create the hard-kill fixture root");
            let database_path = root.join("agent.db");
            let readiness_path = root.join("ready");

            let output = std::process::Command::new(
                std::env::current_exe().expect("current unit test executable"),
            )
            .arg("--exact")
            .arg("store::recovery::tests::boot_physical_receipt_suffix_child")
            .arg("--ignored")
            .arg("--nocapture")
            .env("SUMI_T27_BOUNDARY", boundary)
            .env("SUMI_T27_DATABASE", &database_path)
            .env("SUMI_T27_READY", &readiness_path)
            .output()
            .expect("run the boot physical recovery hard-kill child");
            assert_eq!(
                output.status.code(),
                Some(86),
                "{boundary} child did not exit at the failpoint:\nstdout={}\nstderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                std::fs::read_to_string(&readiness_path).expect("read readiness marker"),
                format!("boot_physical_receipt_suffix.{boundary}\n")
            );

            let reopened = open_boot_running_tools_store(&database_path).await;
            let observed: (i64, String, i64, i64, i64) = sqlx::query_as(
                "SELECT
                    (SELECT COUNT(*) FROM physical_recovery_receipt_applications),
                    (SELECT state FROM tool_executions WHERE tool_call_id = 'tool-boot-kill'),
                    (SELECT COUNT(*) FROM agent_events WHERE event_type = 'turn_end'),
                    (SELECT COUNT(*) FROM agent_events WHERE event_type = 'agent_end'),
                    (SELECT COUNT(*) FROM inbound_commands
                     WHERE status = 'applied' AND run_phase = 'finished')",
            )
            .fetch_one(reopened.pool())
            .await
            .expect("read the restarted state after the hard kill");
            let expected = if boundary == "after_commit" {
                (1, "indeterminate".to_owned(), 1, 1, 1)
            } else {
                (0, "running".to_owned(), 0, 0, 0)
            };
            assert_eq!(
                observed, expected,
                "{boundary} left a torn ledger/terminal/suffix"
            );
            drop(reopened);
            std::fs::remove_dir_all(&root).ok();
        }
    }

    pub(crate) async fn open_boot_running_tools_store(
        database_path: &std::path::Path,
    ) -> Arc<Store> {
        Store::open(
            database_path,
            AgentScope {
                personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
            },
            Arc::new(TestKeyProvider(WrappingKey::new(
                "test",
                [0x61; DATA_KEY_BYTES],
            ))),
        )
        .await
        .expect("open the on-disk boot fixture store")
        .into()
    }

    /// The same ToolUse restart seam, but only the first `running_prefix` calls
    /// ever reached the executor. The rest have no `tool_executions` row at all,
    /// so resolving the physical intents is not enough to close the turn: the
    /// logical suffix must also give those calls their pre-execution result.
    pub(crate) async fn setup_boot_running_tools_with_rowless_tail(
        calls: &[(&str, &str, u32)],
        running_prefix: usize,
    ) -> (Arc<Store>, EventWriter) {
        let (store, writer) = setup().await;
        seed_tool_use_restart_seam_with_assistant(
            &writer,
            false,
            tool_use_recovery_assistant_with_calls(calls),
            &[],
        )
        .await;
        for (tool_call_id, tool_name, slot) in &calls[..running_prefix] {
            persist_running_tool(&writer, tool_call_id, tool_name, *slot).await;
        }
        (store, writer)
    }

    pub(crate) async fn assert_indeterminate_surface(store: &Store, tool_call_id: &str) {
        let (state, error_code): (String, Option<String>) =
            sqlx::query_as("SELECT state, error_code FROM tool_executions WHERE tool_call_id = ?")
                .bind(tool_call_id)
                .fetch_one(store.pool())
                .await
                .expect("load physically recovered tool state");
        assert_eq!(state, "indeterminate");
        assert_eq!(error_code.as_deref(), Some("indeterminate"));

        let envelope: String = sqlx::query_scalar(
            "SELECT envelope FROM agent_events
             WHERE event_type = 'message_end'
               AND json_extract(envelope, '$.message.role') = 'tool_result'
               AND json_extract(envelope, '$.message.tool_call_id') = ?",
        )
        .bind(tool_call_id)
        .fetch_one(store.pool())
        .await
        .expect("load physical recovery ToolResult");
        let envelope: serde_json::Value =
            serde_json::from_str(&envelope).expect("decode ToolResult event");
        assert_eq!(
            envelope
                .pointer("/message/content/0/text")
                .and_then(|v| v.as_str()),
            Some(PHYSICAL_RECOVERY_INDETERMINATE_TOOL_RESULT)
        );
        assert_eq!(
            envelope
                .pointer("/message/details/error")
                .and_then(|v| v.as_str()),
            Some("indeterminate")
        );
    }

    async fn seed_pending_messaging_approval(writer: &EventWriter, turn_id: &str) {
        let request = ApprovalRequest {
            id: TOOL_USE_RECOVERY_APPROVAL_ID.to_owned(),
            tool_call_id: "tool-rowless-messaging".to_owned(),
            tool_name: "messaging".to_owned(),
            action: ReviewProjection::Reviewable(json!({ "operation": "write" })),
            args_summary: json!({ "operation": "write", "place": "general" }),
            reason: None,
            audit: None,
        };
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::approval_requested(request)
                            .expect("pending approval request event"),
                    ),
                    projections: vec![
                        Projection::ToolExecution(ToolExecutionMutation::Prepare {
                            tool_call_id: "tool-rowless-messaging".to_owned(),
                            command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
                            run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                            executor_generation: test_generation(),
                            idempotency_key: format!(
                                "{TOOL_USE_RECOVERY_COMMAND_ID}/tool-rowless-messaging"
                            ),
                        }),
                        Projection::Approval(ApprovalMutation::Pending {
                            request_id: TOOL_USE_RECOVERY_APPROVAL_ID.to_owned(),
                            tool_call_id: "tool-rowless-messaging".to_owned(),
                            run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
                            turn_id: turn_id.to_owned(),
                        }),
                    ],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist pending messaging approval");
    }

    fn error_recovery_assistant(interrupted: bool) -> PublicMessage {
        let PublicMessage::Assistant(mut assistant) = tool_use_recovery_initial_assistant() else {
            unreachable!("assistant fixture")
        };
        if !interrupted {
            assistant.content.clear();
        }
        assistant.stop_reason = StopReason::Error;
        assistant.provider_code = Some("http_503".to_owned());
        assistant.error_message = Some("provider temporarily unavailable".to_owned());
        assistant.interrupted = interrupted;
        PublicMessage::Assistant(assistant)
    }

    #[tokio::test]
    async fn logical_error_recovery_preserves_transcript_and_restarts_at_fixed_point() {
        let root = std::env::temp_dir().join(format!(
            "sumi-logical-error-recovery-{}",
            uuid::Uuid::now_v7()
        ));
        let path = root.join("agent.db");
        let store = open_boot_running_tools_store(&path).await;
        let writer = EventWriter::new(store.clone());
        let assistant = error_recovery_assistant(false);
        seed_tool_use_restart_seam_with_assistant(&writer, false, assistant.clone(), &[]).await;
        store.pool().close().await;
        drop(writer);
        drop(store);

        let restarted = open_boot_running_tools_store(&path).await;
        let (lease, fence, _) = boot_recovery_authority(&restarted);
        let HydrationOutcome::LogicalRecoveryRequired { steps } = restarted
            .hydrate(&lease, &fence)
            .await
            .expect("authenticate the persisted provider error after restart")
        else {
            panic!("unfinished error run requires logical recovery")
        };
        LogicalRecoveryExecutor
            .execute(&restarted, &steps, &lease, &fence)
            .await
            .expect("a persisted provider error must not permanently prevent startup");
        let HydrationOutcome::Complete(hydrated) = restarted
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate the recovered error run")
        else {
            panic!("recovery must reach a complete authenticated state")
        };
        assert_eq!(hydrated.resume, ResumeDirective::AdmitCommands);
        assert!(
            hydrated.messages.iter().any(|message| {
                matches!(message, ContextMessage::Persisted { id, .. }
                if id == TOOL_USE_RECOVERY_ASSISTANT_ID)
                    && crate::memory::overflow::context_message_to_public(message) == assistant
            }),
            "the original provider error must remain in the authenticated transcript"
        );
        assert_eq!(
            sqlx::query_as::<_, (String, String, i64, i64, i64)>(
                "SELECT status, run_phase,
                    (SELECT COUNT(*) FROM agent_events WHERE event_type='turn_end'),
                    (SELECT COUNT(*) FROM agent_events WHERE event_type='agent_end'),
                    (SELECT COUNT(*) FROM messages)
                 FROM inbound_commands WHERE command_id=?",
            )
            .bind(TOOL_USE_RECOVERY_COMMAND_ID)
            .fetch_one(restarted.pool())
            .await
            .expect("inspect recovered lifecycle"),
            ("applied".to_owned(), "finished".to_owned(), 1, 1, 2),
        );
        let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(restarted.pool())
            .await
            .expect("event count after recovery");
        restarted.pool().close().await;
        drop(restarted);

        let second_restart = open_boot_running_tools_store(&path).await;
        assert!(matches!(
            second_restart
                .hydrate(&lease, &fence)
                .await
                .expect("second restart"),
            HydrationOutcome::Complete(_)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(second_restart.pool())
                .await
                .expect("event count after second restart"),
            event_count,
            "restarting again must not duplicate the recovery suffix"
        );
        second_restart.pool().close().await;
        std::fs::remove_dir_all(root).expect("remove the disposable recovery fixture");
    }

    #[tokio::test]
    async fn logical_error_recovery_closes_the_current_continuation_during_retry_wait() {
        let (store, writer) = setup().await;
        let assistant = error_recovery_assistant(true);
        seed_tool_use_restart_seam_with_assistant(&writer, true, assistant.clone(), &[]).await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::retry_scheduled(
                            TOOL_USE_RECOVERY_RUN_ID,
                            TOOL_USE_RECOVERY_CONTINUATION_TURN_ID,
                            1,
                            2_000,
                            Utc::now() + chrono::Duration::seconds(2),
                            "provider temporarily unavailable",
                        )
                        .expect("retry schedule"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist retry wait before restart");
        let (lease, fence, _) = boot_recovery_authority(&store);
        let HydrationOutcome::LogicalRecoveryRequired { steps } = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate retry wait")
        else {
            panic!("retry wait needs recovery")
        };
        LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect("close the interrupted continuation");
        assert!(matches!(
            store
                .hydrate(&lease, &fence)
                .await
                .expect("hydrate after recovery"),
            HydrationOutcome::Complete(_)
        ));
        let (turn_ends, retries, encoded): (i64, i64, String) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM agent_events WHERE event_type='turn_end'),
                (SELECT COUNT(*) FROM agent_events WHERE event_type='retry_scheduled'),
                json_extract(envelope, '$.message')
             FROM agent_events WHERE event_type='turn_end'
               AND json_extract(internal_metadata, '$.turn_id')=?",
        )
        .bind(TOOL_USE_RECOVERY_CONTINUATION_TURN_ID)
        .fetch_one(store.pool())
        .await
        .expect("recovered continuation");
        assert_eq!((turn_ends, retries), (2, 1));
        assert_eq!(
            serde_json::from_str::<PublicMessage>(&encoded).expect("error terminal"),
            assistant
        );
    }

    #[tokio::test]
    async fn logical_error_recovery_cannot_close_a_later_unfinished_attempt() {
        let (store, writer) = setup().await;
        seed_tool_use_restart_seam_with_assistant(
            &writer,
            false,
            error_recovery_assistant(false),
            &[],
        )
        .await;
        writer
            .apply(EventBatch {
                writes: vec![
                    EventWrite {
                        event: Some(
                            DurableEvent::retry_scheduled(
                                TOOL_USE_RECOVERY_RUN_ID,
                                TOOL_USE_RECOVERY_TURN_ID,
                                1,
                                0,
                                Utc::now(),
                                "provider temporarily unavailable",
                            )
                            .expect("retry schedule"),
                        ),
                        projections: Vec::new(),
                    },
                    EventWrite {
                        event: Some(
                            DurableEvent::message_in_turn(
                                "message_start",
                                "later-assistant-attempt",
                                &tool_use_recovery_initial_assistant(),
                                Some(TOOL_USE_RECOVERY_RUN_ID.to_owned()),
                                Some(TOOL_USE_RECOVERY_TURN_ID.to_owned()),
                            )
                            .expect("later assistant start"),
                        ),
                        projections: Vec::new(),
                    },
                ],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist later attempt without a terminal");
        let (lease, fence, _) = boot_recovery_authority(&store);
        let HydrationOutcome::LogicalRecoveryRequired { steps } = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate incomplete retry")
        else {
            panic!("incomplete retry needs recovery")
        };
        let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("event count before rejected recovery");
        let error = LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect_err("old Error must not stand in for a later unfinished attempt");
        assert!(error.to_string().contains("later attempt"), "{error:#}");
        let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("event count after rejected recovery");
        assert_eq!(before, after);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM inbound_commands WHERE command_id=?",
            )
            .bind(TOOL_USE_RECOVERY_COMMAND_ID)
            .fetch_one(store.pool())
            .await
            .expect("owner status"),
            "applying"
        );
    }

    #[tokio::test]
    async fn logical_tool_use_recovery_reuses_terminal_results_skips_rowless_call_and_restarts_at_fixed_point()
     {
        let root = std::env::temp_dir().join(format!(
            "sumi-logical-tool-use-recovery-{}",
            uuid::Uuid::now_v7()
        ));
        let path = root.join("agent.db");
        let scope = AgentScope {
            personality_agent_id: test_personality_agent_id(),
        };
        let provider: Arc<dyn super::super::KeyProvider> = Arc::new(TestKeyProvider(
            WrappingKey::new("test", [0x61; DATA_KEY_BYTES]),
        ));
        let store: Arc<Store> = Store::open(&path, scope.clone(), provider.clone())
            .await
            .expect("open first restart fixture")
            .into();
        let writer = EventWriter::new(store.clone());
        seed_tool_use_restart_seam(&writer, false).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM agent_events WHERE event_type='tool_execution_start'",
            )
            .fetch_one(store.pool())
            .await
            .expect("pre-restart ToolExecutionStart count"),
            2
        );
        store.pool().close().await;
        drop(writer);
        drop(store);

        let first_restart: Arc<Store> = Store::open(&path, scope.clone(), provider.clone())
            .await
            .expect("open first logical-recovery restart")
            .into();
        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-lease",
        )
        .expect("logical-recovery lease");
        let fence = GenerationRecoveryFence::new(&lease, "logical-recovery-fence")
            .expect("logical-recovery fence");
        let HydrationOutcome::LogicalRecoveryRequired { steps } = first_restart
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate first restart")
        else {
            panic!("first restart must require the ToolUse logical suffix")
        };
        assert!(matches!(
            steps.as_slice(),
            [RecoveryStep::ResumeAssistantFromDurableEvents {
                command_id,
                run_id,
                turn_id,
                pending_error_context: None,
            }] if command_id == TOOL_USE_RECOVERY_COMMAND_ID
                && run_id == TOOL_USE_RECOVERY_RUN_ID
                && turn_id == TOOL_USE_RECOVERY_TURN_ID
        ));

        let events_before_unsupported: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_events")
                .fetch_one(first_restart.pool())
                .await
                .expect("event count before unsupported plan");
        let mut unsupported = steps.clone();
        unsupported.push(RecoveryStep::Reclassify {
            command_id: "00000000-0000-4000-8000-000000000092".to_owned(),
        });
        let error = LogicalRecoveryExecutor
            .execute(&first_restart, &unsupported, &lease, &fence)
            .await
            .expect_err("mixed recovery plan must fail before mutation");
        assert!(error.to_string().contains("only supports one"), "{error:#}");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(first_restart.pool())
                .await
                .expect("event count after unsupported plan"),
            events_before_unsupported
        );

        LogicalRecoveryExecutor
            .execute(&first_restart, &steps, &lease, &fence)
            .await
            .expect("close authenticated ToolUse restart seam");

        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>)>(
                "SELECT state, error_code FROM tool_executions
                 WHERE tool_call_id='tool-rowless-messaging'",
            )
            .fetch_one(first_restart.pool())
            .await
            .expect("synthetic rowless tool disposition"),
            (
                "not_started".to_owned(),
                Some("process_restarted".to_owned())
            )
        );
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT
                   (SELECT state FROM tool_executions
                    WHERE tool_call_id='tool-terminal-success'),
                   (SELECT state FROM tool_executions
                    WHERE tool_call_id='tool-terminal-failure')",
            )
            .fetch_one(first_restart.pool())
            .await
            .expect("terminal tool states after recovery"),
            ("succeeded".to_owned(), "failed".to_owned())
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT
                   (SELECT COUNT(*) FROM agent_events
                    WHERE event_type='tool_execution_start'),
                   (SELECT COUNT(*) FROM agent_events
                    WHERE event_type='tool_execution_end'),
                   (SELECT COUNT(*) FROM agent_events
                    WHERE event_type='tool_execution_start'
                      AND json_extract(envelope, '$.tool_call_id')='tool-rowless-messaging')",
            )
            .fetch_one(first_restart.pool())
            .await
            .expect("tool event counts after recovery"),
            (2, 2, 0),
            "recovery must neither re-execute terminal calls nor execute the rowless call"
        );

        let synthetic_message_id =
            tool_result_message_id(TOOL_USE_RECOVERY_ASSISTANT_ID, "tool-rowless-messaging");
        let mut authentication = first_restart
            .pool()
            .begin()
            .await
            .expect("begin recovered transcript authentication");
        super::super::event_writer::authenticate_event_log_snapshot(
            &first_restart,
            &mut authentication,
        )
        .await
        .expect("authenticate recovered event snapshot");
        let messages = first_restart
            .hydrate_messages(&mut authentication)
            .await
            .expect("hydrate recovered transcript");
        authentication
            .commit()
            .await
            .expect("commit recovered transcript authentication");
        let synthetic_result = messages
            .iter()
            .find_map(|message| match message {
                ContextMessage::Persisted {
                    id,
                    message: Message::ToolResult(result),
                    ..
                } if id == &synthetic_message_id => Some(result),
                _ => None,
            })
            .expect("synthetic restart ToolResult");
        assert_eq!(synthetic_result.tool_call_id, "tool-rowless-messaging");
        assert_eq!(synthetic_result.tool_name, "messaging");
        assert!(synthetic_result.is_error);
        assert_eq!(
            synthetic_result.content,
            vec![UserContent::Text {
                text: PROCESS_RESTARTED_TOOL_RESULT.to_owned(),
            }]
        );
        assert_eq!(
            synthetic_result.details,
            json!({ "error": PROCESS_RESTARTED_TOOL_RESULT })
        );
        assert_eq!(
            sqlx::query_as::<_, (String, String, i64, i64, i64)>(
                "SELECT
                   (SELECT status FROM inbound_commands
                    WHERE command_id=?),
                   (SELECT run_phase FROM inbound_commands
                    WHERE command_id=?),
                   (SELECT COUNT(*) FROM agent_events WHERE event_type='turn_end'),
                   (SELECT COUNT(*) FROM agent_events WHERE event_type='agent_end'),
                   (SELECT json_array_length(json_extract(envelope, '$.tool_results'))
                    FROM agent_events WHERE event_type='turn_end'
                      AND json_extract(internal_metadata, '$.turn_id')=?)",
            )
            .bind(TOOL_USE_RECOVERY_COMMAND_ID)
            .bind(TOOL_USE_RECOVERY_COMMAND_ID)
            .bind(TOOL_USE_RECOVERY_TURN_ID)
            .fetch_one(first_restart.pool())
            .await
            .expect("recovered lifecycle closure"),
            ("applied".to_owned(), "finished".to_owned(), 1, 1, 3)
        );
        first_restart.pool().close().await;
        drop(first_restart);

        let second_restart = Store::open(&path, scope, provider)
            .await
            .expect("open second restart");
        assert!(matches!(
            second_restart
                .hydrate(&lease, &fence)
                .await
                .expect("hydrate second restart"),
            HydrationOutcome::Complete(_)
        ));
        second_restart.pool().close().await;
        drop(second_restart);
        std::fs::remove_dir_all(root).expect("remove logical ToolUse recovery fixture");
    }

    #[tokio::test]
    async fn pending_approval_recovery_closes_the_authenticated_continuation_turn() {
        let (store, writer) = setup().await;
        seed_tool_use_restart_seam(&writer, true).await;
        seed_pending_messaging_approval(&writer, TOOL_USE_RECOVERY_CONTINUATION_TURN_ID).await;
        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-continuation-lease",
        )
        .expect("logical-recovery lease");
        let fence = GenerationRecoveryFence::new(&lease, "logical-recovery-continuation-fence")
            .expect("logical-recovery fence");
        let HydrationOutcome::LogicalRecoveryRequired { steps } = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate continuation-turn restart")
        else {
            panic!("continuation-turn restart must require the ToolUse logical suffix")
        };
        assert!(matches!(
            steps.as_slice(),
            [RecoveryStep::CancelPendingApproval {
                command_id,
                run_id,
                turn_id,
                request_id,
                tool_call_id,
            }] if command_id == TOOL_USE_RECOVERY_COMMAND_ID
                && run_id == TOOL_USE_RECOVERY_RUN_ID
                && turn_id == TOOL_USE_RECOVERY_CONTINUATION_TURN_ID
                && request_id == TOOL_USE_RECOVERY_APPROVAL_ID
                && tool_call_id == "tool-rowless-messaging"
        ));

        LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect("cancel authenticated continuation approval seam");

        assert_eq!(
            sqlx::query_as::<_, (String, Option<String>, String, i64)>(
                "SELECT
                   t.state,
                   t.error_code,
                   a.state,
                   (SELECT COUNT(*) FROM agent_events
                    WHERE event_type='tool_execution_start'
                      AND json_extract(envelope, '$.tool_call_id')='tool-rowless-messaging')
                 FROM tool_executions AS t
                 JOIN approval_log AS a ON a.tool_call_id=t.tool_call_id
                 WHERE t.tool_call_id='tool-rowless-messaging'",
            )
            .fetch_one(store.pool())
            .await
            .expect("continuation approval disposition"),
            (
                "cancelled".to_owned(),
                Some(APPROVAL_CANCELLED_ERROR_CODE.to_owned()),
                "cancelled".to_owned(),
                0,
            )
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64)>(
                "SELECT
                   (SELECT COUNT(*) FROM agent_events WHERE event_type='turn_end'
                      AND json_extract(internal_metadata, '$.turn_id')=?),
                   (SELECT COUNT(*) FROM agent_events WHERE event_type='turn_end'
                      AND json_extract(internal_metadata, '$.turn_id')=?),
                   (SELECT COUNT(*) FROM agent_events WHERE event_type='agent_end')",
            )
            .bind(TOOL_USE_RECOVERY_TURN_ID)
            .bind(TOOL_USE_RECOVERY_CONTINUATION_TURN_ID)
            .fetch_one(store.pool())
            .await
            .expect("continuation lifecycle closure"),
            (1, 1, 1),
            "recovery must preserve owner turn A and close only active turn B"
        );
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT status, run_phase FROM inbound_commands WHERE command_id=?",
            )
            .bind(TOOL_USE_RECOVERY_COMMAND_ID)
            .fetch_one(store.pool())
            .await
            .expect("continuation owner terminal state"),
            ("applied".to_owned(), "finished".to_owned())
        );

        let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("event count after approval recovery");
        assert!(matches!(
            store
                .hydrate(&lease, &fence)
                .await
                .expect("rehydrate recovered approval seam"),
            HydrationOutcome::Complete(_)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count after fixed-point hydration"),
            event_count,
            "fixed-point hydration must not duplicate the cancellation suffix"
        );
    }

    #[tokio::test]
    async fn pending_approval_recovery_rejects_rowless_call_before_pending_without_mutation() {
        let (store, writer) = setup().await;
        let assistant = tool_use_recovery_assistant_with_calls(&[
            ("tool-rowless-before-pending", "read_file", 0),
            ("tool-rowless-messaging", "messaging", 1),
        ]);
        seed_tool_use_restart_seam_with_assistant(&writer, true, assistant, &[]).await;
        seed_pending_messaging_approval(&writer, TOOL_USE_RECOVERY_CONTINUATION_TURN_ID).await;
        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-result-order-lease",
        )
        .expect("logical-recovery lease");
        let fence = GenerationRecoveryFence::new(&lease, "logical-recovery-result-order-fence")
            .expect("logical-recovery fence");
        let HydrationOutcome::LogicalRecoveryRequired { steps } = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate rowless-before-pending restart")
        else {
            panic!("rowless-before-pending restart must require logical recovery")
        };
        let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("event count before rejected recovery");

        LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect_err("a later pending call cannot follow an earlier rowless call");

        assert_eq!(
            sqlx::query_as::<_, (i64, i64, i64, String, String)>(
                "SELECT
                   (SELECT COUNT(*) FROM agent_events),
                   (SELECT COUNT(*) FROM messages WHERE role='tool_result'),
                   (SELECT COUNT(*) FROM tool_executions
                    WHERE tool_call_id='tool-rowless-before-pending'),
                   (SELECT state FROM tool_executions
                    WHERE tool_call_id='tool-rowless-messaging'),
                   (SELECT state FROM approval_log WHERE id=?)",
            )
            .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
            .fetch_one(store.pool())
            .await
            .expect("durable state after rejected out-of-order recovery"),
            (
                event_count,
                0,
                0,
                "prepared".to_owned(),
                "pending".to_owned()
            ),
            "rejection must not append events, synthesize results, or disposition either call"
        );
    }

    #[tokio::test]
    async fn pending_approval_recovery_rejects_a_mismatched_request_without_mutation() {
        let (store, writer) = setup().await;
        seed_tool_use_restart_seam(&writer, true).await;
        seed_pending_messaging_approval(&writer, TOOL_USE_RECOVERY_CONTINUATION_TURN_ID).await;
        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-wrong-approval-lease",
        )
        .expect("logical-recovery lease");
        let fence = GenerationRecoveryFence::new(&lease, "logical-recovery-wrong-approval-fence")
            .expect("logical-recovery fence");
        let HydrationOutcome::LogicalRecoveryRequired { mut steps } = store
            .hydrate(&lease, &fence)
            .await
            .expect("hydrate pending approval restart")
        else {
            panic!("pending approval restart must require logical recovery")
        };
        let RecoveryStep::CancelPendingApproval { request_id, .. } = &mut steps[0] else {
            panic!("pending approval restart must plan cancellation")
        };
        *request_id = "different-approval-request".to_owned();
        let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("event count before rejected cancellation");

        let error = LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect_err("mismatched approval request must fail closed");
        assert!(
            error
                .to_string()
                .contains("pending approval does not match the typed recovery step"),
            "{error:#}"
        );
        assert_eq!(
            sqlx::query_as::<_, (i64, String, String)>(
                "SELECT
                   (SELECT COUNT(*) FROM agent_events),
                   (SELECT state FROM approval_log WHERE id=?),
                   (SELECT state FROM tool_executions WHERE tool_call_id='tool-rowless-messaging')",
            )
            .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
            .fetch_one(store.pool())
            .await
            .expect("durable state after rejected cancellation"),
            (event_count, "pending".to_owned(), "prepared".to_owned())
        );
    }

    #[tokio::test]
    async fn pending_approval_recovery_rejects_missing_projection_rows_without_mutation() {
        let (store, writer) = setup().await;
        seed_tool_use_restart_seam(&writer, true).await;
        seed_pending_messaging_approval(&writer, TOOL_USE_RECOVERY_CONTINUATION_TURN_ID).await;
        let mut transaction = store
            .pool()
            .begin()
            .await
            .expect("begin missing approval projection fixture");
        sqlx::query("DELETE FROM approval_log WHERE id=?")
            .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
            .execute(&mut *transaction)
            .await
            .expect("delete pending approval projection");
        sqlx::query("DELETE FROM tool_executions WHERE tool_call_id='tool-rowless-messaging'")
            .execute(&mut *transaction)
            .await
            .expect("delete prepared tool projection");
        transaction
            .commit()
            .await
            .expect("commit missing approval projection fixture");

        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-missing-approval-projection-lease",
        )
        .expect("logical-recovery lease");
        let fence = GenerationRecoveryFence::new(
            &lease,
            "logical-recovery-missing-approval-projection-fence",
        )
        .expect("logical-recovery fence");
        let state_before = sqlx::query_as::<_, (i64, i64, i64, i64, String)>(
            "SELECT
               (SELECT COUNT(*) FROM agent_events),
               (SELECT COUNT(*) FROM messages WHERE role='tool_result'),
               (SELECT COUNT(*) FROM approval_log WHERE id=?),
               (SELECT COUNT(*) FROM tool_executions
                WHERE tool_call_id='tool-rowless-messaging'),
               (SELECT status FROM inbound_commands WHERE command_id=?)",
        )
        .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
        .bind(TOOL_USE_RECOVERY_COMMAND_ID)
        .fetch_one(store.pool())
        .await
        .expect("state before missing approval projection recovery");

        let rejected = match store.hydrate(&lease, &fence).await {
            Err(_) => true,
            Ok(HydrationOutcome::LogicalRecoveryRequired { steps }) => LogicalRecoveryExecutor
                .execute(&store, &steps, &lease, &fence)
                .await
                .is_err(),
            Ok(_) => false,
        };
        let state_after = sqlx::query_as::<_, (i64, i64, i64, i64, String)>(
            "SELECT
               (SELECT COUNT(*) FROM agent_events),
               (SELECT COUNT(*) FROM messages WHERE role='tool_result'),
               (SELECT COUNT(*) FROM approval_log WHERE id=?),
               (SELECT COUNT(*) FROM tool_executions
                WHERE tool_call_id='tool-rowless-messaging'),
               (SELECT status FROM inbound_commands WHERE command_id=?)",
        )
        .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
        .bind(TOOL_USE_RECOVERY_COMMAND_ID)
        .fetch_one(store.pool())
        .await
        .expect("state after missing approval projection recovery");

        assert_eq!(
            state_after, state_before,
            "missing projections must not let recovery replace authenticated pending approval evidence with a rowless result"
        );
        assert!(
            rejected,
            "authenticated unresolved ApprovalRequested evidence must fail closed when both mutable projection rows are missing"
        );
    }

    #[tokio::test]
    async fn pending_approval_recovery_rejects_forged_orphan_projection_without_mutation() {
        const FORGED_REQUEST_ID: &str = "approval-forged-orphan-recovery";
        const FORGED_TOOL_CALL_ID: &str = "tool-forged-orphan-recovery";

        let (store, writer) = setup().await;
        seed_tool_use_restart_seam(&writer, true).await;
        seed_pending_messaging_approval(&writer, TOOL_USE_RECOVERY_CONTINUATION_TURN_ID).await;
        sqlx::query(
            "INSERT INTO approval_log
             (id, tool_call_id, run_id, turn_id, state, request_projection, redaction_version, created_at)
             VALUES (?, ?, ?, ?, 'pending', '{}', 1, ?)",
        )
        .bind(FORGED_REQUEST_ID)
        .bind(FORGED_TOOL_CALL_ID)
        .bind(TOOL_USE_RECOVERY_RUN_ID)
        .bind(TOOL_USE_RECOVERY_CONTINUATION_TURN_ID)
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .expect("insert forged orphan pending approval projection");

        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-forged-orphan-approval-lease",
        )
        .expect("logical-recovery lease");
        let fence =
            GenerationRecoveryFence::new(&lease, "logical-recovery-forged-orphan-approval-fence")
                .expect("logical-recovery fence");
        let state_before = sqlx::query_as::<_, (i64, i64, String, String, String, String)>(
            "SELECT
               (SELECT COUNT(*) FROM agent_events),
               (SELECT COUNT(*) FROM messages WHERE role='tool_result'),
               (SELECT state FROM approval_log WHERE id=?),
               (SELECT state FROM tool_executions
                WHERE tool_call_id='tool-rowless-messaging'),
               (SELECT state FROM approval_log WHERE id=?),
               (SELECT status FROM inbound_commands WHERE command_id=?)",
        )
        .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
        .bind(FORGED_REQUEST_ID)
        .bind(TOOL_USE_RECOVERY_COMMAND_ID)
        .fetch_one(store.pool())
        .await
        .expect("state before forged orphan approval recovery");

        let rejected = match store.hydrate(&lease, &fence).await {
            Err(_) => true,
            Ok(HydrationOutcome::LogicalRecoveryRequired { steps }) => LogicalRecoveryExecutor
                .execute(&store, &steps, &lease, &fence)
                .await
                .is_err(),
            Ok(_) => false,
        };
        let state_after = sqlx::query_as::<_, (i64, i64, String, String, String, String)>(
            "SELECT
               (SELECT COUNT(*) FROM agent_events),
               (SELECT COUNT(*) FROM messages WHERE role='tool_result'),
               (SELECT state FROM approval_log WHERE id=?),
               (SELECT state FROM tool_executions
                WHERE tool_call_id='tool-rowless-messaging'),
               (SELECT state FROM approval_log WHERE id=?),
               (SELECT status FROM inbound_commands WHERE command_id=?)",
        )
        .bind(TOOL_USE_RECOVERY_APPROVAL_ID)
        .bind(FORGED_REQUEST_ID)
        .bind(TOOL_USE_RECOVERY_COMMAND_ID)
        .fetch_one(store.pool())
        .await
        .expect("state after forged orphan approval recovery");

        assert_eq!(
            state_after, state_before,
            "an orphan pending approval projection must not survive while recovery closes its run"
        );
        assert!(
            rejected,
            "pending approval projections must exactly match authenticated lifecycle request/tool evidence"
        );
    }

    #[tokio::test]
    async fn logical_tool_use_recovery_without_an_authenticated_open_turn_fails_atomically() {
        let (store, writer) = setup().await;
        seed_tool_use_restart_seam(&writer, true).await;
        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "logical-recovery-no-open-turn-lease",
        )
        .expect("logical-recovery lease");
        let fence = GenerationRecoveryFence::new(&lease, "logical-recovery-no-open-turn-fence")
            .expect("logical-recovery fence");
        let steps = vec![RecoveryStep::ResumeAssistantFromDurableEvents {
            command_id: TOOL_USE_RECOVERY_COMMAND_ID.to_owned(),
            run_id: TOOL_USE_RECOVERY_RUN_ID.to_owned(),
            turn_id: TOOL_USE_RECOVERY_TURN_ID.to_owned(),
            pending_error_context: None,
        }];
        LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect("first recovery closes the authenticated run");
        let event_count_before = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
            .fetch_one(store.pool())
            .await
            .expect("event count before rejected recovery");
        let tool_count_before =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_executions")
                .fetch_one(store.pool())
                .await
                .expect("tool count before rejected recovery");

        let error = LogicalRecoveryExecutor
            .execute(&store, &steps, &lease, &fence)
            .await
            .expect_err("recovery without an authenticated open turn must fail");
        assert!(
            error.to_string().contains("has no authenticated open turn"),
            "{error:#}"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count after rejected recovery"),
            event_count_before
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tool_executions")
                .fetch_one(store.pool())
                .await
                .expect("tool count after rejected recovery"),
            tool_count_before
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT status FROM inbound_commands WHERE command_id=?",
            )
            .bind(TOOL_USE_RECOVERY_COMMAND_ID)
            .fetch_one(store.pool())
            .await
            .expect("command status after rejected recovery"),
            "applied"
        );
    }

    async fn seed_pending_approval_decision(
        store: &Store,
        writer: &EventWriter,
        command_id: &str,
        request_id: &str,
        decision: ApprovalDecision,
    ) {
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: crate::gateway::CommandId::parse(command_id).expect("command ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::ApprovalDecision {
                    request_id: request_id.to_owned(),
                    decision,
                },
            }))
            .await
            .expect("persist approval decision before simulated restart");

        let suffix = request_id
            .strip_prefix("request-approve-")
            .unwrap_or(request_id);
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO approval_log
             (id, tool_call_id, run_id, turn_id, state, request_projection, redaction_version, created_at)
             VALUES (?, ?, ?, ?, 'pending', '{}', 1, ?)",
        )
        .bind(request_id)
        .bind(format!("tool-call-{suffix}"))
        .bind(format!("run-{suffix}"))
        .bind(format!("turn-{suffix}"))
        .bind(now)
        .execute(store.pool())
        .await
        .expect("seed pending approval log");
    }

    async fn persist_user(writer: &EventWriter, seq: u64, id: &str) {
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq,
                command_id: crate::gateway::CommandId::parse(id)
                    .expect("test command_id must be canonical"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::UserMessage {
                    text: id.to_owned(),
                    attachments: Vec::new(),
                },
            }))
            .await
            .expect("persist user");
    }

    async fn persist_run_started(store: &Store, writer: &EventWriter) {
        let seq = u64::try_from(
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq),0) + 1 FROM inbound_commands")
                .fetch_one(store.pool())
                .await
                .expect("next recovery fixture sequence"),
        )
        .expect("next recovery fixture sequence");
        persist_user(writer, seq, "00000000-0000-4000-8000-000000000001").await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-1".to_owned(),
                        turn_id: "turn-1".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify recovery fixture");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(
                            &serde_json::json!({"type":"agent_start","run_id":"run-1"}),
                        )
                        .expect("AgentStart"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-1".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist authenticated AgentStart");
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000001'",
            )
            .fetch_one(store.pool())
            .await
            .expect("stored phase"),
            "run_started"
        );
    }

    async fn persist_valid_history(
        store: &Store,
        writer: &EventWriter,
        minimum_event_count: usize,
    ) -> usize {
        // Each cycle persists AgentStart, AgentEnd, and the authoritative
        // dispositions for both the user command and its abort.
        const EVENTS_PER_CYCLE: usize = 4;
        let cycle_count = minimum_event_count.div_ceil(EVENTS_PER_CYCLE);
        let mut next_seq = u64::try_from(
            sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(seq),0) FROM inbound_commands")
                .fetch_one(store.pool())
                .await
                .expect("load next command sequence"),
        )
        .expect("stored command sequence")
        .saturating_add(1);
        for _ in 0..cycle_count {
            let user_id = Uuid::now_v7().to_string();
            let abort_id = Uuid::now_v7().to_string();
            let run_id = Uuid::now_v7().to_string();
            let turn_id = Uuid::now_v7().to_string();
            persist_user(writer, next_seq, &user_id).await;
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::CommandClassified {
                            command_id: user_id.clone(),
                            application_kind: ApplicationKind::IdleRun,
                            run_id: run_id.clone(),
                            turn_id,
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("classify valid history command");
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: Some(
                            DurableEvent::new(&serde_json::json!({
                                "type":"agent_start",
                                "run_id":run_id
                            }))
                            .expect("valid history AgentStart"),
                        ),
                        projections: vec![Projection::RunPhase {
                            command_id: user_id,
                            run_id,
                            expected: RunPhase::Classified,
                            next: RunPhase::RunStarted,
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("persist valid history AgentStart");
            next_seq = next_seq.saturating_add(1);
            writer
                .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                    seq: next_seq,
                    command_id: crate::gateway::CommandId::parse(&abort_id)
                        .expect("valid history Abort command ID"),
                    personality_agent_id: test_personality_agent_id(),
                    provenance: test_provenance(),
                    command: Command::Abort {},
                }))
                .await
                .expect("persist valid history Abort");
            writer
                .apply_idle_abort_cutoff(&abort_id, next_seq)
                .await
                .expect("close valid history run");
            next_seq = next_seq.saturating_add(1);
        }
        cycle_count * EVENTS_PER_CYCLE
    }

    #[tokio::test]
    async fn authenticated_event_head_accepts_valid_history_across_page_boundary() {
        let (store, writer) = setup().await;
        let persisted =
            persist_valid_history(&store, &writer, EVENT_EVIDENCE_PAGE_ROWS as usize + 1).await;

        assert!(
            SuffixRecovery::plan(&store)
                .await
                .expect("valid page-boundary history")
                .is_empty()
        );
        let head: (i64, i64) = sqlx::query_as("SELECT last_seq,event_count FROM event_log_heads")
            .fetch_one(store.pool())
            .await
            .expect("event-log head");
        assert_eq!(
            head,
            (
                i64::try_from(persisted).expect("persisted event count"),
                i64::try_from(persisted).expect("persisted event count"),
            )
        );
    }

    #[tokio::test]
    async fn authenticated_event_head_rejects_middle_and_tail_deletion() {
        let (middle_store, middle_writer) = setup().await;
        persist_valid_history(&middle_store, &middle_writer, 4).await;
        sqlx::query("DELETE FROM agent_events WHERE seq=2")
            .execute(middle_store.pool())
            .await
            .expect("delete middle event");
        let error = SuffixRecovery::plan(&middle_store)
            .await
            .expect_err("middle deletion must fail");
        assert!(
            error.to_string().contains("durable event sequence gap"),
            "{error:#}"
        );

        let (tail_store, tail_writer) = setup().await;
        persist_valid_history(&tail_store, &tail_writer, 4).await;
        sqlx::query("DELETE FROM agent_events WHERE seq=4")
            .execute(tail_store.pool())
            .await
            .expect("delete tail event");
        let error = SuffixRecovery::plan(&tail_store)
            .await
            .expect_err("tail deletion must fail");
        assert!(
            error
                .to_string()
                .contains("does not match authenticated head"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn authenticated_event_head_rejects_head_metadata_mismatch() {
        let (store, writer) = setup().await;
        persist_valid_history(&store, &writer, 4).await;
        sqlx::query("UPDATE event_log_heads SET chain_digest=zeroblob(32)")
            .execute(store.pool())
            .await
            .expect("tamper event-log head");

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("head mismatch must fail");
        assert!(
            format!("{error:#}").contains("event-log head HMAC mismatch"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn plans_only_the_next_missing_suffix_from_phase() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;
        let steps = SuffixRecovery::plan(&store).await.expect("plan received");
        assert_eq!(
            steps,
            vec![RecoveryStep::Reclassify {
                command_id: "00000000-0000-4000-8000-000000000001".to_owned()
            }]
        );

        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-1".to_owned(),
                        turn_id: "turn-1".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify");
        assert_eq!(
            SuffixRecovery::plan(&store).await.expect("plan classified"),
            vec![RecoveryStep::EmitAgentStart {
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                run_id: "run-1".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn t12_prefix_application_classifies_idle_then_leaves_t17_suffix_pending() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;

        let pending = SuffixRecovery::recover_t12_prefix(&store, &writer)
            .await
            .expect("recover T12-owned prefix");
        assert!(matches!(
            pending.as_slice(),
            [RecoveryStep::EmitAgentStart { command_id, .. }]
                if command_id == "00000000-0000-4000-8000-000000000001"
        ));
        let row = sqlx::query(
            "SELECT status, application_kind, run_id, turn_id, run_phase
             FROM inbound_commands WHERE seq=1",
        )
        .fetch_one(store.pool())
        .await
        .expect("recovered command");
        assert_eq!(row.get::<String, _>("status"), "applying");
        assert_eq!(row.get::<String, _>("application_kind"), "idle_run");
        assert!(!row.get::<String, _>("run_id").is_empty());
        assert!(!row.get::<String, _>("turn_id").is_empty());
        assert_eq!(row.get::<String, _>("run_phase"), "classified");
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_events")
                .fetch_one(store.pool())
                .await
                .expect("event count"),
            0,
            "T17-owned full-suffix AgentStart must remain pending after T12 prefix application"
        );
    }

    #[tokio::test]
    async fn t12_prefix_application_applies_idle_abort_cutoff_before_reclassifying() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 2,
                command_id: crate::gateway::CommandId::parse(
                    "00000000-0000-4000-8000-000000000002",
                )
                .expect("command ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::Abort {},
            }))
            .await
            .expect("persist Abort before simulated restart");

        assert!(
            SuffixRecovery::recover_t12_prefix(&store, &writer)
                .await
                .expect("apply idle Abort cutoff")
                .is_empty()
        );
        let states = sqlx::query(
            "SELECT status, application_kind, run_phase
             FROM inbound_commands ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .expect("terminal command states");
        assert_eq!(states[0].get::<String, _>("status"), "superseded");
        assert!(
            states[0]
                .get::<Option<String>, _>("application_kind")
                .is_none(),
            "Abort cutoff must close the still-unclassified command"
        );
        assert_eq!(states[0].get::<String, _>("run_phase"), "received");
        assert_eq!(states[1].get::<String, _>("status"), "applied");
    }

    #[tokio::test]
    async fn startup_abort_contextualizes_earlier_unclassified_users_without_binding_them() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000011").await;
        persist_user(&writer, 2, "00000000-0000-4000-8000-000000000012").await;
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-startup',
                 turn_id='turn-startup', run_phase='classified'
             WHERE seq=2",
        )
        .execute(store.pool())
        .await
        .expect("seed invalid startup classification");
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 3,
                command_id: crate::gateway::CommandId::parse(
                    "00000000-0000-4000-8000-000000000013",
                )
                .expect("Abort ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::Abort {},
            }))
            .await
            .expect("persist startup Abort");

        SuffixRecovery::recover_t12_prefix(&store, &writer)
            .await
            .expect_err("non-idle startup classification must fail without partial terminals");
        assert_eq!(
            sqlx::query_as::<_, (String, String, String)>(
                "SELECT
                    (SELECT status FROM inbound_commands WHERE seq=1),
                    (SELECT status FROM inbound_commands WHERE seq=2),
                    (SELECT status FROM inbound_commands WHERE seq=3)",
            )
            .fetch_one(store.pool())
            .await
            .expect("failed recovery states"),
            (
                "received".to_owned(),
                "applying".to_owned(),
                "received".to_owned()
            )
        );

        sqlx::query("UPDATE inbound_commands SET application_kind='idle_run' WHERE seq=2")
            .execute(store.pool())
            .await
            .expect("repair startup fixture");
        assert!(
            SuffixRecovery::recover_t12_prefix(&store, &writer)
                .await
                .expect("recover contextual startup Abort")
                .is_empty()
        );
        let rows = sqlx::query(
            "SELECT status, run_id, application_kind, run_phase
             FROM inbound_commands ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .expect("recovered command rows");
        assert_eq!(rows[0].get::<String, _>("status"), "superseded");
        assert!(
            rows[0].get::<Option<String>, _>("run_id").is_none(),
            "contextual supersede must not become a durable classification binding"
        );
        assert!(
            rows[0]
                .get::<Option<String>, _>("application_kind")
                .is_none()
        );
        assert_eq!(rows[0].get::<String, _>("run_phase"), "received");
        assert_eq!(rows[1].get::<String, _>("status"), "superseded");
        assert_eq!(rows[2].get::<String, _>("status"), "applied");
    }

    #[tokio::test]
    async fn t12_prefix_application_terminals_unknown_approval_as_durable_noop() {
        let (store, writer) = setup().await;
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: crate::gateway::CommandId::parse(
                    "00000000-0000-4000-8000-000000000001",
                )
                .expect("command ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::ApprovalDecision {
                    request_id: "unknown-request".to_owned(),
                    decision: ApprovalDecision::DenyOnce,
                },
            }))
            .await
            .expect("persist ApprovalDecision before simulated restart");

        assert!(
            SuffixRecovery::recover_t12_prefix(&store, &writer)
                .await
                .expect("apply unknown approval no-op")
                .is_empty()
        );
        assert_eq!(
            writer
                .ack_for_command("00000000-0000-4000-8000-000000000001")
                .await
                .expect("durable ACK")
                .expect("command row")
                .status,
            crate::gateway::CommandAckStatus::Applied
        );
    }

    #[tokio::test]
    async fn t12_prefix_preserves_pending_approved_once_for_recovery() {
        let (store, writer) = setup().await;
        let command_id = "00000000-0000-4000-8000-000000000002";
        // Simulate a crash where the approval decision was received but the
        // tool was never started: approval_log is still pending and the command
        // must not be silently applied as a no-op.
        seed_pending_approval_decision(
            &store,
            &writer,
            command_id,
            "request-approve-once",
            ApprovalDecision::ApproveOnce,
        )
        .await;

        let steps = SuffixRecovery::recover_t12_prefix(&store, &writer)
            .await
            .expect("plan recovery for pending approved tool");
        assert_eq!(
            steps.len(),
            1,
            "pending approval must produce a recovery step"
        );
        assert!(
            matches!(steps[0], RecoveryStep::ApplyControl { command_id: ref id } if id == command_id),
            "pending approved decision must return ApplyControl, not no-op: {steps:?}"
        );

        let status: String =
            sqlx::query_scalar("SELECT status FROM inbound_commands WHERE command_id=?")
                .bind(command_id)
                .fetch_one(store.pool())
                .await
                .expect("command row");
        assert_eq!(
            status, "received",
            "approval decision must remain received while pending"
        );

        let state: String = sqlx::query_scalar("SELECT state FROM approval_log WHERE id=?")
            .bind("request-approve-once")
            .fetch_one(store.pool())
            .await
            .expect("approval log row");
        assert_eq!(
            state, "pending",
            "approval log must remain pending for recovery"
        );
    }

    #[tokio::test]
    async fn t12_prefix_preserves_pending_approved_always_for_recovery() {
        let (store, writer) = setup().await;
        let command_id = "00000000-0000-4000-8000-000000000003";
        let rule = json!({
            "id": "rule-fixture-always",
            "tool": "bash",
            "literal_prefix": ["git", "status"],
            "effect": "allow",
            "workspace_only": true,
            "allowed_permissions": ["exec"],
            "allowed_network_domains": []
        });
        seed_pending_approval_decision(
            &store,
            &writer,
            command_id,
            "request-approve-always",
            ApprovalDecision::ApproveAlways {
                rule: serde_json::from_value::<DeferredApprovalRule>(rule).expect("deferred rule"),
            },
        )
        .await;

        let steps = SuffixRecovery::recover_t12_prefix(&store, &writer)
            .await
            .expect("plan recovery for pending approved-always tool");
        assert_eq!(
            steps.len(),
            1,
            "pending approval must produce a recovery step"
        );
        assert!(
            matches!(steps[0], RecoveryStep::ApplyControl { command_id: ref id } if id == command_id),
            "pending approved-always decision must return ApplyControl, not no-op: {steps:?}"
        );

        let state: String = sqlx::query_scalar("SELECT state FROM approval_log WHERE id=?")
            .bind("request-approve-always")
            .fetch_one(store.pool())
            .await
            .expect("approval log row");
        assert_eq!(
            state, "pending",
            "approval log must remain pending for recovery"
        );
    }

    #[tokio::test]
    async fn no_pending_commands_still_require_authenticated_event_history() {
        let (store, writer) = setup().await;
        persist_valid_history(&store, &writer, 2).await;
        sqlx::query("UPDATE agent_events SET raw_ciphertext=zeroblob(1)")
            .execute(store.pool())
            .await
            .expect("corrupt history that recovery must not read");

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("history corruption is fatal even without pending commands");
        assert!(
            format!("{error:#}").contains("failed authenticated recovery"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn pending_recovery_authenticates_every_keyset_page_without_retaining_history() {
        let (store, writer) = setup().await;
        persist_valid_history(&store, &writer, EVENT_EVIDENCE_PAGE_ROWS as usize * 2 + 1).await;
        persist_run_started(&store, &writer).await;
        sqlx::query(
            "UPDATE agent_events SET raw_ciphertext=zeroblob(1)
             WHERE seq=(SELECT MAX(seq) FROM agent_events)",
        )
        .execute(store.pool())
        .await
        .expect("corrupt the final keyset page");

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("pending recovery must authenticate the final page");
        assert!(error.to_string().contains("failed authenticated recovery"));
    }

    #[tokio::test]
    async fn validates_event_evidence_instead_of_inventing_terminal_suffix() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: None,
                    projections: vec![Projection::CommandClassified {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        application_kind: ApplicationKind::IdleRun,
                        run_id: "run-1".to_owned(),
                        turn_id: "turn-1".to_owned(),
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("classify");
        sqlx::query(
            "UPDATE inbound_commands SET run_phase='run_started' WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("phase fixture");
        assert!(
            SuffixRecovery::plan(&store)
                .await
                .unwrap_err()
                .to_string()
                .contains("no durable AgentStart evidence")
        );

        sqlx::query(
            "UPDATE inbound_commands SET run_phase='classified' WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("restore pre-transition fixture");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(
                            &serde_json::json!({"type":"agent_start","run_id":"run-1"}),
                        )
                        .expect("event"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-1".to_owned(),
                        expected: RunPhase::Classified,
                        next: RunPhase::RunStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("append evidence");
        assert_eq!(
            SuffixRecovery::plan(&store).await.expect("plan suffix"),
            vec![RecoveryStep::EmitTurnStart {
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                run_id: "run-1".to_owned(),
                turn_id: "turn-1".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn recovery_rejects_valid_forged_redacted_projection() {
        let (store, writer) = setup().await;
        persist_run_started(&store, &writer).await;
        sqlx::query(
            "UPDATE agent_events
             SET envelope='{\"type\":\"agent_start\",\"run_id\":\"run-1\",\"forged\":true}'",
        )
        .execute(store.pool())
        .await
        .expect("install valid forged JSON projection");

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("projection cannot supply recovery evidence");
        assert!(
            error
                .to_string()
                .contains("projection does not match authenticated raw event")
        );
    }

    #[tokio::test]
    async fn recovery_rejects_forged_internal_event_metadata() {
        let (store, writer) = setup().await;
        persist_run_started(&store, &writer).await;
        sqlx::query(
            "UPDATE agent_events
             SET internal_metadata='{\"run_id\":\"forged-run\"}'
             WHERE seq=1",
        )
        .execute(store.pool())
        .await
        .expect("forge internal event metadata");

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("internal metadata is part of the authenticated event chain");
        assert!(
            error
                .to_string()
                .contains("does not match authenticated head"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn recovery_rejects_raw_ciphertext_row_swap_and_key_ref_substitution() {
        let (store, writer) = setup().await;
        persist_run_started(&store, &writer).await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::turn_start("run-1", "turn-1")
                            .expect("second valid lifecycle event"),
                    ),
                    projections: vec![Projection::RunPhase {
                        command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                        run_id: "run-1".to_owned(),
                        expected: RunPhase::RunStarted,
                        next: RunPhase::TurnStarted,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist second authenticated event");
        let rows = sqlx::query("SELECT seq, raw_ciphertext FROM agent_events ORDER BY seq")
            .fetch_all(store.pool())
            .await
            .expect("read ciphertext fixtures");
        let first_seq: i64 = rows[0].try_get("seq").expect("first seq");
        let second_seq: i64 = rows[1].try_get("seq").expect("second seq");
        let first: Vec<u8> = rows[0].try_get("raw_ciphertext").expect("first ciphertext");
        let second: Vec<u8> = rows[1]
            .try_get("raw_ciphertext")
            .expect("second ciphertext");
        let mut transaction = store.pool().begin().await.expect("swap transaction");
        sqlx::query("UPDATE agent_events SET raw_ciphertext=? WHERE seq=?")
            .bind(&second)
            .bind(first_seq)
            .execute(&mut *transaction)
            .await
            .expect("swap first");
        sqlx::query("UPDATE agent_events SET raw_ciphertext=? WHERE seq=?")
            .bind(&first)
            .bind(second_seq)
            .execute(&mut *transaction)
            .await
            .expect("swap second");
        transaction.commit().await.expect("commit swap");
        assert!(
            SuffixRecovery::plan(&store)
                .await
                .expect_err("row-swapped raw events must not authenticate")
                .to_string()
                .contains("failed authenticated recovery")
        );

        sqlx::query("UPDATE agent_events SET raw_ciphertext=? WHERE seq=?")
            .bind(&first)
            .bind(first_seq)
            .execute(store.pool())
            .await
            .expect("restore first ciphertext");
        sqlx::query("UPDATE agent_events SET raw_ciphertext=? WHERE seq=?")
            .bind(&second)
            .bind(second_seq)
            .execute(store.pool())
            .await
            .expect("restore second ciphertext");
        let transcript = store
            .private_key(DataKeyPurpose::Transcript)
            .await
            .expect("mint wrong-purpose key");
        sqlx::query("UPDATE agent_events SET raw_key_ref=? WHERE seq=?")
            .bind(&transcript.key_ref)
            .bind(first_seq)
            .execute(store.pool())
            .await
            .expect("substitute key ref");
        assert!(
            SuffixRecovery::plan(&store)
                .await
                .expect_err("non-event key ref must fail closed")
                .to_string()
                .contains("non-event data key")
        );
    }

    #[tokio::test]
    async fn recovery_rejects_aad_sequence_identity_tamper() {
        let (store, writer) = setup().await;
        persist_run_started(&store, &writer).await;
        sqlx::query("UPDATE agent_events SET seq=seq+100")
            .execute(store.pool())
            .await
            .expect("move physical event sequence");

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("AAD sequence substitution must not authenticate");
        assert!(
            error.to_string().contains("durable event sequence gap"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn authenticated_recovery_evidence_survives_valid_store_reopen() {
        let root =
            std::env::temp_dir().join(format!("sumi-recovery-reopen-{}", uuid::Uuid::now_v7()));
        let path = root.join("agent.db");
        let scope = AgentScope {
            personality_agent_id: "0198f0f4-9b72-7000-8000-000000000001".parse().unwrap(),
        };
        let provider: Arc<dyn super::super::KeyProvider> = Arc::new(TestKeyProvider(
            WrappingKey::new("test", [0x61; DATA_KEY_BYTES]),
        ));
        let store: Arc<Store> = Store::open(&path, scope.clone(), provider.clone())
            .await
            .expect("open file store")
            .into();
        let writer = EventWriter::new(store.clone());
        persist_run_started(&store, &writer).await;
        store.pool().close().await;
        drop(writer);
        drop(store);

        let reopened = Store::open(&path, scope, provider)
            .await
            .expect("reopen store");
        assert_eq!(
            SuffixRecovery::plan(&reopened)
                .await
                .expect("authenticate reopened evidence"),
            vec![RecoveryStep::EmitTurnStart {
                command_id: "00000000-0000-4000-8000-000000000001".to_owned(),
                run_id: "run-1".to_owned(),
                turn_id: "turn-1".to_owned(),
            }]
        );
        reopened.pool().close().await;
        std::fs::remove_dir_all(root).expect("remove reopen fixture");
    }

    #[tokio::test]
    async fn assistant_and_cancel_recovery_are_phase_specific_not_fixed_endings() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='idle_run', run_id='run-1',
                 turn_id='turn-1', run_phase='assistant_started'
             WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("assistant phase fixture");
        assert!(matches!(
            SuffixRecovery::plan(&store).await.expect("assistant plan")[0],
            RecoveryStep::ResumeAssistantFromDurableEvents { .. }
        ));

        sqlx::query(
            "UPDATE inbound_commands SET run_phase='cancel_requested'
             WHERE command_id='00000000-0000-4000-8000-000000000001'",
        )
        .execute(store.pool())
        .await
        .expect("cancel phase fixture");
        assert!(matches!(
            SuffixRecovery::plan(&store).await.expect("cancel plan")[0],
            RecoveryStep::ResumeCancellationFromDurableEvents { .. }
        ));
    }

    #[tokio::test]
    async fn full_suffix_plan_collects_all_pending_steps_in_order() {
        let (store, writer) = setup().await;
        persist_run_started(&store, &writer).await;
        persist_user(&writer, 2, "00000000-0000-4000-8000-000000000002").await;

        let lease = ProcessGenerationLease::new(
            test_personality_agent_id(),
            test_generation(),
            "full-suffix-plan-lease",
        )
        .expect("full suffix plan lease");
        let fence = GenerationRecoveryFence::new(&lease, "full-suffix-plan-fence")
            .expect("full suffix plan fence");
        let recovery = writer
            .begin_bootstrap_recovery(&lease, &fence)
            .await
            .expect("authenticate full suffix lifecycle");
        let steps = SuffixRecovery::plan_full_suffix(&store, &recovery)
            .await
            .expect("full suffix plan");

        assert_eq!(steps.len(), 2);
        assert!(matches!(
            &steps[0],
            RecoveryStep::EmitTurnStart {
                command_id,
                run_id,
                turn_id,
            } if command_id == "00000000-0000-4000-8000-000000000001"
                && run_id == "run-1"
                && turn_id == "turn-1"
        ));
        assert!(matches!(
            &steps[1],
            RecoveryStep::Reclassify { command_id }
                if command_id == "00000000-0000-4000-8000-000000000002"
        ));
    }

    #[tokio::test]
    async fn pending_window_is_bounded_and_abort_is_planned_before_earlier_work() {
        let (abort_store, abort_writer) = setup().await;
        persist_user(&abort_writer, 1, "00000000-0000-4000-8000-000000000001").await;
        persist_user(&abort_writer, 2, "00000000-0000-4000-8000-000000000002").await;
        abort_writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 3,
                command_id: crate::gateway::CommandId::parse(
                    "00000000-0000-4000-8000-000000000003",
                )
                .expect("Abort ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::Abort {},
            }))
            .await
            .expect("persist Abort");
        assert_eq!(
            SuffixRecovery::plan(&abort_store)
                .await
                .expect("Abort-priority plan"),
            vec![RecoveryStep::ApplyControl {
                command_id: "00000000-0000-4000-8000-000000000003".to_owned()
            }]
        );

        let (oversized_store, oversized_writer) = setup().await;
        for seq in 1..=128 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            persist_user(&oversized_writer, seq, &id).await;
        }
        let error = SuffixRecovery::plan(&oversized_store).await.expect_err(
            "128 nonterminal commands must fail closed without whole-history materialization",
        );
        assert!(
            error
                .to_string()
                .contains("ordinary command window exceeds 32 commands"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn pending_window_accepts_thirty_two_ordinary_commands_plus_reserved_abort() {
        let (store, writer) = setup().await;
        for seq in 1..=PENDING_COMMAND_MAX_COUNT as u64 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            persist_user(&writer, seq, &id).await;
        }
        let abort_seq = PENDING_COMMAND_MAX_COUNT as u64 + 1;
        let abort_id = format!("00000000-0000-4000-8000-{abort_seq:012x}");
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: abort_seq,
                command_id: crate::gateway::CommandId::parse(&abort_id).expect("Abort ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::Abort {},
            }))
            .await
            .expect("persist reserved Abort");

        assert_eq!(
            SuffixRecovery::plan(&store)
                .await
                .expect("exact count boundary with reserved Abort"),
            vec![RecoveryStep::ApplyControl {
                command_id: abort_id
            }]
        );
    }

    #[tokio::test]
    async fn pending_window_rejects_more_than_one_reserved_abort() {
        let (store, writer) = setup().await;
        for seq in 1..=2 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            writer
                .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                    seq,
                    command_id: crate::gateway::CommandId::parse(&id).expect("Abort ID"),
                    personality_agent_id: test_personality_agent_id(),
                    provenance: test_provenance(),
                    command: Command::Abort {},
                }))
                .await
                .expect("persist pending Abort");
        }

        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("a second pending Abort must fail closed");
        assert!(
            error.to_string().contains("more than one pending Abort"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn pending_window_accepts_exact_four_mib_ordinary_payload_plus_abort() {
        let (store, writer) = setup().await;
        let per_command_bytes = PENDING_COMMAND_MAX_BYTES / 4;
        for seq in 1..=4 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            let empty = serde_json::to_vec(&Command::UserMessage {
                text: String::new(),
                attachments: Vec::new(),
            })
            .expect("serialize empty command")
            .len();
            let command = Command::UserMessage {
                text: "x".repeat(per_command_bytes - empty),
                attachments: Vec::new(),
            };
            assert_eq!(
                serde_json::to_vec(&command)
                    .expect("serialize exact-boundary command")
                    .len(),
                per_command_bytes
            );
            writer
                .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                    seq,
                    command_id: crate::gateway::CommandId::parse(&id).expect("command ID"),
                    personality_agent_id: test_personality_agent_id(),
                    provenance: test_provenance(),
                    command,
                }))
                .await
                .expect("persist exact-boundary ordinary command");
        }
        let abort_id = "00000000-0000-4000-8000-000000000005";
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 5,
                command_id: crate::gateway::CommandId::parse(abort_id).expect("Abort ID"),
                personality_agent_id: test_personality_agent_id(),
                provenance: test_provenance(),
                command: Command::Abort {},
            }))
            .await
            .expect("persist reserved Abort");

        assert_eq!(
            SuffixRecovery::plan(&store)
                .await
                .expect("exact byte boundary with reserved Abort"),
            vec![RecoveryStep::ApplyControl {
                command_id: abort_id.to_owned()
            }]
        );
    }

    #[tokio::test]
    async fn pending_window_rejects_canonical_plaintext_over_four_mib() {
        let (store, writer) = setup().await;
        for seq in 1..=5 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            writer
                .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                    seq,
                    command_id: crate::gateway::CommandId::parse(&id).expect("command ID"),
                    personality_agent_id: test_personality_agent_id(),
                    provenance: test_provenance(),
                    command: Command::UserMessage {
                        text: "x".repeat(900 * 1024),
                        attachments: Vec::new(),
                    },
                }))
                .await
                .expect("persist authenticated large pending command");
        }
        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("oversized canonical pending window must fail closed");
        assert!(error.to_string().contains("canonical bytes"), "{error:#}");
    }

    #[tokio::test]
    async fn bounded_group_accepts_sixteen_and_rejects_seventeenth_member() {
        let (store, writer) = setup().await;
        let mut ids = Vec::new();
        for seq in 1..=RECOVERY_GROUP_MAX_COMMANDS as u64 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            persist_user(&writer, seq, &id).await;
            ids.push(id);
        }
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-1',
                 turn_id='turn-2', run_phase='classified'",
        )
        .execute(store.pool())
        .await
        .expect("prepare 16-command durable group");
        assert_eq!(
            SuffixRecovery::plan(&store)
                .await
                .expect("bounded valid group"),
            vec![RecoveryStep::InjectStoredGroup {
                run_id: "run-1".to_owned(),
                turn_id: "turn-2".to_owned(),
                application_kind: ApplicationKind::SoftSteer,
                command_ids: ids,
            }]
        );

        let id = "00000000-0000-4000-8000-000000000011";
        persist_user(&writer, 17, id).await;
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-1',
                 turn_id='turn-2', run_phase='classified' WHERE seq=17",
        )
        .execute(store.pool())
        .await
        .expect("append seventeenth durable group member");
        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("seventeenth group member must fail closed");
        assert!(
            error.to_string().contains("exceeds 16 commands"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn bounded_group_rejects_plaintext_over_one_mib_while_materializing() {
        let (store, writer) = setup().await;
        for seq in 1..=2 {
            let id = format!("00000000-0000-4000-8000-{seq:012x}");
            writer
                .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                    seq,
                    command_id: crate::gateway::CommandId::parse(&id).expect("command ID"),
                    personality_agent_id: test_personality_agent_id(),
                    provenance: test_provenance(),
                    command: Command::UserMessage {
                        text: "x".repeat(600 * 1024),
                        attachments: Vec::new(),
                    },
                }))
                .await
                .expect("persist authenticated group member");
        }
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-1',
                 turn_id='turn-2', run_phase='classified'",
        )
        .execute(store.pool())
        .await
        .expect("prepare oversized durable group");
        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("oversized durable group must fail closed");
        assert!(
            error.to_string().contains("canonical plaintext bytes"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn grouped_recovery_is_sequence_ordered_and_rejects_mixed_phases() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;
        persist_user(&writer, 2, "00000000-0000-4000-8000-000000000002").await;
        sqlx::query(
            "UPDATE inbound_commands
             SET status='applying', application_kind='soft_steer', run_id='run-1',
                 turn_id='turn-2', run_phase='classified'
             WHERE command_id IN ('00000000-0000-4000-8000-000000000001', '00000000-0000-4000-8000-000000000002')",
        )
        .execute(store.pool())
        .await
        .expect("prepare durable steer group");

        assert_eq!(
            SuffixRecovery::plan(&store).await.expect("plan group"),
            vec![RecoveryStep::InjectStoredGroup {
                run_id: "run-1".to_owned(),
                turn_id: "turn-2".to_owned(),
                application_kind: ApplicationKind::SoftSteer,
                command_ids: vec![
                    "00000000-0000-4000-8000-000000000001".to_owned(),
                    "00000000-0000-4000-8000-000000000002".to_owned()
                ],
            }]
        );

        sqlx::query(
            "UPDATE inbound_commands SET run_phase='turn_started'
             WHERE command_id='00000000-0000-4000-8000-000000000002'",
        )
        .execute(store.pool())
        .await
        .expect("install mixed-phase crash fixture");
        let error = SuffixRecovery::plan(&store)
            .await
            .expect_err("one durable group cannot contain mixed phases");
        assert!(error.to_string().contains("mixed phases"));
    }
}
