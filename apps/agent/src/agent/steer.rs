//! Durable steering classification, cancellation, and group injection.
//!
//! T16 owns the active/live classification boundary, the hard-steer step-zero
//! commit-before-cancel contract, partial assistant finalization, soft/retry
//! group snapshot injection with owner hand-off, and abort/supersede cutoff.
//!
//! This module deliberately builds EventBatch values against the already-frozen
//! T12 EventWriter projection schema; it does not itself perform I/O.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Result, anyhow, bail};
use tokio_util::sync::CancellationToken;

use crate::{
    gateway::Command,
    provider::types::{PublicAssistantContent, PublicMessage, StopReason, ToolResultMessage},
    store::{
        ApplicationKind, DurableEvent, EventBatch, EventWrite, InjectedCommand, Projection,
        Redactor, RunPhase,
    },
};

use super::{AdmittedCommand, DurableRunBinding, SteerMode};

/// Observable phase of a live run from the Session's point of view.  This is
/// distinct from the durable `RunPhase` stored in `inbound_commands`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SteerStage {
    /// Assistant text/thinking/tool-call generation is in flight.
    AssistantGeneration,
    /// One or more tool calls are executing or awaiting approval.
    ToolOrApproval,
    /// The run is between provider attempts during retry backoff.
    RetryWait,
    /// No steerable live phase (Idle or between turns without a cancel token).
    Other,
}

impl SteerStage {
    /// Classify a user command received while the run is in this stage.
    pub(crate) fn classify_user_command(self) -> Option<ApplicationKind> {
        match self {
            Self::AssistantGeneration => Some(ApplicationKind::HardSteer),
            Self::ToolOrApproval => Some(ApplicationKind::SoftSteer),
            Self::RetryWait => Some(ApplicationKind::RetrySteer),
            Self::Other => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Provider-attempt cancellation registry (hard-steer step zero)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct AttemptState {
    token: Option<CancellationToken>,
}

/// Session-visible handle for the one provider attempt currently owned by a
/// run worker. It contains no conversation state and is never held across an
/// await. Taking the token makes the post-commit signal one-shot.
#[derive(Debug, Default)]
pub(crate) struct AttemptCancellation {
    state: Mutex<AttemptState>,
    reservation_active: AtomicBool,
    hard_steer_committed: AtomicBool,
}

impl AttemptCancellation {
    pub(crate) fn register(self: &Arc<Self>, token: CancellationToken) -> Result<AttemptGuard> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("attempt cancellation registry is poisoned"))?;
        if state.token.is_some() || self.reservation_active.load(Ordering::Acquire) {
            bail!("a provider attempt cancellation token is already registered");
        }
        state.token = Some(token);
        Ok(AttemptGuard {
            _owner: self.clone(),
        })
    }

    /// Retires a normally completed attempt after its durable MessageEnd.
    pub(crate) fn retire_committed(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("attempt cancellation registry is poisoned"))?;
        if self.reservation_active.load(Ordering::Acquire) {
            bail!("cannot retire a reserved provider attempt");
        }
        state.token.take();
        self.hard_steer_committed.store(false, Ordering::Release);
        Ok(())
    }

    /// Takes exclusive ownership of the active token without signalling it.
    pub(crate) fn reserve(self: &Arc<Self>) -> Result<AttemptReservation> {
        if self
            .reservation_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            bail!("provider attempt cancellation token is already reserved");
        }
        let token = match self.state.lock() {
            Ok(mut state) => match state.token.take() {
                Some(token) => token,
                None => {
                    self.reservation_active.store(false, Ordering::Release);
                    bail!("hard steer committed without an active provider attempt");
                }
            },
            Err(_) => {
                self.reservation_active.store(false, Ordering::Release);
                bail!("attempt cancellation registry is poisoned");
            }
        };
        Ok(AttemptReservation {
            owner: self.clone(),
            token: Some(token),
        })
    }

    pub(crate) fn hard_steer_committed(&self) -> bool {
        self.hard_steer_committed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn has_registered_attempt(&self) -> bool {
        self.state.lock().expect("attempt state").token.is_some()
    }
}

pub(crate) struct AttemptGuard {
    _owner: Arc<AttemptCancellation>,
}

/// Pre-commit reservation of the active provider token. Dropping an
/// uncommitted reservation restores it, while `cancel_after_commit` is
/// deliberately infallible and performs no registry lookup.
pub(crate) struct AttemptReservation {
    owner: Arc<AttemptCancellation>,
    token: Option<CancellationToken>,
}

impl AttemptReservation {
    pub(crate) fn restore(mut self) -> Result<()> {
        let mut state = self
            .owner
            .state
            .lock()
            .map_err(|_| anyhow!("attempt cancellation registry is poisoned"))?;
        if !self.owner.reservation_active.load(Ordering::Acquire) || state.token.is_some() {
            bail!("provider attempt cancellation reservation state changed");
        }
        let token = self
            .token
            .take()
            .expect("live reservation retains its cancellation token");
        state.token = Some(token);
        self.owner
            .reservation_active
            .store(false, Ordering::Release);
        Ok(())
    }

    pub(crate) fn cancel_after_commit(mut self) {
        self.owner
            .hard_steer_committed
            .store(true, Ordering::Release);
        self.token
            .take()
            .expect("committed reservation retains its cancellation token")
            .cancel();
        self.owner
            .reservation_active
            .store(false, Ordering::Release);
    }
}

impl Drop for AttemptReservation {
    fn drop(&mut self) {
        let Some(token) = self.token.take() else {
            return;
        };
        if let Ok(mut state) = self.owner.state.lock()
            && self.owner.reservation_active.load(Ordering::Acquire)
            && state.token.is_none()
        {
            state.token = Some(token);
        }
        self.owner
            .reservation_active
            .store(false, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// Hard-steer durable batches
// ---------------------------------------------------------------------------

/// Receipt returned after the step-zero transaction commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HardSteerReceipt {
    pub(crate) run_id: String,
    pub(crate) turn_id: String,
}

/// Build the step-zero EventBatch: classify the new command as a hard steer
/// and advance the current owner to `hard_steer_requested` in one transaction.
/// The caller must cancel the provider attempt only after `writer.apply`
/// returns successfully.
pub(crate) fn hard_steer_step_zero_batch(
    owner: &DurableRunBinding,
    command: &AdmittedCommand,
    turn_id: impl Into<String>,
) -> Result<EventBatch> {
    if !matches!(command.envelope().command, Command::UserMessage { .. }) {
        bail!("only UserMessage may cross the hard-steer barrier");
    }
    let turn_id = turn_id.into();
    Ok(EventBatch {
        writes: vec![EventWrite {
            event: None,
            projections: vec![
                Projection::CommandClassified {
                    command_id: command.envelope().command_id.to_string(),
                    application_kind: ApplicationKind::HardSteer,
                    run_id: owner.run_id.clone(),
                    turn_id: turn_id.clone(),
                },
                Projection::RunPhase {
                    command_id: owner.command_id.clone(),
                    run_id: owner.run_id.clone(),
                    expected: RunPhase::AssistantStarted,
                    next: RunPhase::HardSteerRequested,
                },
            ],
        }],
        injected_commands: Vec::new(),
    })
}

/// Finalize a partial assistant message produced by a hard-steered provider
/// attempt and inject the steering user message as the new owner.
///
/// The returned batches, in order, are:
///   1. `MessageEnd`(partial assistant, interrupted=true, old owner retained)
///      -> `TurnEnd` -> `Steered { mode: Hard }` -> `TurnStart(new turn)`
///   2. `MessageStart/End`(user) with atomic owner hand-off.
///
/// Splitting into two batches is required because the partial assistant
/// `MessageEnd` must see exactly one live owner; the new user message cannot
/// open ownership until after that boundary.
pub(crate) fn finalize_hard_steer_batches(
    owner: &DurableRunBinding,
    command: &AdmittedCommand,
    partial_message_id: String,
    partial: PublicMessage,
    new_turn_id: impl Into<String>,
) -> Result<Vec<EventBatch>> {
    if !matches!(command.envelope().command, Command::UserMessage { .. }) {
        bail!("only UserMessage may finalize a hard steer");
    }
    let new_turn_id = new_turn_id.into();
    let message = normalize_partial_assistant(partial)?;

    // Batch 1: partial assistant MessageEnd (old owner retained) + TurnEnd.
    let close_batch = EventBatch {
        writes: vec![
            EventWrite {
                event: Some(DurableEvent::message_in_turn(
                    "message_end",
                    &partial_message_id,
                    &message,
                    Some(owner.run_id.clone()),
                    Some(owner.turn_id.clone()),
                )?),
                projections: vec![Projection::MessageEnd {
                    message_id: partial_message_id,
                    role: "assistant",
                    message: message.clone(),
                    append_to_l0: true,
                    provider_context: Vec::new(),
                    eviction_footprint_tokens: 0,
                }],
            },
            EventWrite {
                event: Some(DurableEvent::turn_end(
                    &owner.run_id,
                    &owner.turn_id,
                    message,
                    Vec::new(),
                )?),
                projections: Vec::new(),
            },
        ],
        injected_commands: Vec::new(),
    };

    // Batch 2: Steered, TurnStart, user MessageStart/End with atomic owner hand-off.
    let user_message = build_user_message(command)?;
    let user_message_id = crate::store::user_message_id(&command.envelope().command_id);
    let previous_owner_id = owner.command_id.clone();
    let previous_owner_seq = owner.command_seq;
    let new_command_id = command.envelope().command_id.to_string();
    let new_command_seq = command.envelope().seq;

    let inject_batch = EventBatch {
        writes: vec![
            EventWrite {
                event: Some(DurableEvent::steered(
                    SteerMode::Hard,
                    command.envelope().command_id.to_string(),
                    owner.run_id.clone(),
                    new_turn_id.clone(),
                )?),
                projections: vec![Projection::RunPhase {
                    command_id: command.envelope().command_id.to_string(),
                    run_id: owner.run_id.clone(),
                    expected: RunPhase::Classified,
                    next: RunPhase::TurnStarted,
                }],
            },
            EventWrite {
                event: Some(DurableEvent::turn_start(&owner.run_id, &new_turn_id)?),
                projections: Vec::new(),
            },
            EventWrite {
                event: Some(DurableEvent::message(
                    "message_start",
                    &user_message_id,
                    &user_message,
                )?),
                projections: vec![
                    Projection::CommandApplied {
                        command_id: previous_owner_id,
                        command_seq: previous_owner_seq,
                        run_id: Some(owner.run_id.clone()),
                    },
                    Projection::RunPhase {
                        command_id: new_command_id.clone(),
                        run_id: owner.run_id.clone(),
                        expected: RunPhase::TurnStarted,
                        next: RunPhase::UserStarted,
                    },
                ],
            },
            EventWrite {
                event: Some(DurableEvent::message(
                    "message_end",
                    &user_message_id,
                    &user_message,
                )?),
                projections: vec![
                    Projection::MessageEnd {
                        message_id: user_message_id,
                        role: "user",
                        message: user_message,
                        append_to_l0: true,
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
                    },
                    Projection::RunPhase {
                        command_id: new_command_id,
                        run_id: owner.run_id.clone(),
                        expected: RunPhase::UserStarted,
                        next: RunPhase::UserCommitted,
                    },
                ],
            },
        ],
        injected_commands: vec![InjectedCommand::new(
            new_command_seq,
            command.envelope().command_id.clone(),
        )],
    };

    Ok(vec![close_batch, inject_batch])
}

pub(crate) fn normalize_partial_assistant(partial: PublicMessage) -> Result<PublicMessage> {
    let PublicMessage::Assistant(mut assistant) = partial else {
        bail!("hard-steer partial message must be an assistant message");
    };

    // Keep completed text and verified complete thinking; drop unverified
    // partial thinking and every tool-call block (executed or not).
    let mut kept = Vec::with_capacity(assistant.content.len());
    for content in assistant.content {
        match content {
            PublicAssistantContent::Text {
                text,
                wire_item_index,
            } => {
                if !text.is_empty() {
                    kept.push(PublicAssistantContent::Text {
                        text,
                        wire_item_index,
                    });
                }
            }
            // Verified complete thinking only.  The adapter layer is
            // responsible for deciding whether a thinking block is complete
            // enough to survive (signature present for Anthropic, any partial
            // for Kimi).  Here we only drop the obviously-invalid cases.
            PublicAssistantContent::Thinking {
                thinking,
                signature_field,
                wire_item_index,
            } => {
                if !thinking.is_empty() || !signature_field.is_empty() {
                    kept.push(PublicAssistantContent::Thinking {
                        thinking,
                        signature_field,
                        wire_item_index,
                    });
                }
            }
            PublicAssistantContent::ToolCall { .. }
            | PublicAssistantContent::RejectedToolCall { .. } => {
                // Unexecuted calls are not part of the durable transcript.
            }
        }
    }

    // Append the interruption marker so the model recognizes the cutoff.
    let marker = "[この応答はユーザーの割り込みにより中断された]";
    if let Some(PublicAssistantContent::Text { text, .. }) = kept.last_mut() {
        if !text.ends_with(marker) {
            *text = format!("{text}\n\n{marker}");
        }
    } else {
        kept.push(PublicAssistantContent::Text {
            text: marker.to_owned(),
            wire_item_index: u32::MAX,
        });
    }

    assistant.content = kept;
    assistant.interrupted = true;
    assistant.stop_reason = StopReason::Aborted;
    Ok(PublicMessage::Assistant(assistant))
}

// ---------------------------------------------------------------------------
// Soft/retry group injection
// ---------------------------------------------------------------------------

/// A snapshot of classified soft/retry steer commands waiting for an injection
/// boundary. Once snapshot, later arrivals are not part of this group.
#[derive(Clone, Debug)]
pub(crate) struct SteerGroupSnapshot {
    pub(crate) application_kind: ApplicationKind,
    pub(crate) run_id: String,
    pub(crate) turn_id: String,
    pub(crate) previous_owner: DurableRunBinding,
    pub(crate) commands: Vec<AdmittedCommand>,
    /// For soft steer: the assistant message that closed the previous turn,
    /// used to durably emit `TurnEnd` before the new turn starts.
    pub(crate) closing_turn_message: Option<PublicMessage>,
    /// For soft steer: the tool results that closed the previous turn.
    pub(crate) closing_tool_results: Vec<ToolResultMessage>,
}

/// Accumulate classified soft/retry steer commands until the worker reaches an
/// injection boundary.
#[derive(Debug)]
pub(crate) struct SteerGroup {
    application_kind: ApplicationKind,
    run_id: String,
    turn_id: String,
    commands: Vec<AdmittedCommand>,
    plaintext_bytes: usize,
}

impl SteerGroup {
    pub(crate) fn new(
        application_kind: ApplicationKind,
        run_id: impl Into<String>,
        turn_id: impl Into<String>,
    ) -> Result<Self> {
        if !matches!(
            application_kind,
            ApplicationKind::SoftSteer | ApplicationKind::RetrySteer
        ) {
            bail!("SteerGroup only accepts soft or retry steer");
        }
        Ok(Self {
            application_kind,
            run_id: run_id.into(),
            turn_id: turn_id.into(),
            commands: Vec::new(),
            plaintext_bytes: 0,
        })
    }

    pub(crate) fn application_kind(&self) -> ApplicationKind {
        self.application_kind
    }

    pub(crate) fn turn_id(&self) -> &str {
        &self.turn_id
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.commands.len()
    }

    pub(crate) fn commands(&self) -> &[AdmittedCommand] {
        &self.commands
    }

    /// Returns true if the command can join this group.  The caller must pass
    /// the durable run/turn the command was classified into.
    pub(crate) fn can_accept(
        &self,
        command: &AdmittedCommand,
        run_id: &str,
        turn_id: &str,
        application_kind: ApplicationKind,
    ) -> bool {
        run_id == self.run_id
            && turn_id == self.turn_id
            && application_kind == self.application_kind
            && matches!(command.envelope().command, Command::UserMessage { .. })
    }

    /// Returns true if adding `command` would stay within the group bounds.
    pub(crate) fn can_accept_with_size(
        &self,
        command: &AdmittedCommand,
        run_id: &str,
        turn_id: &str,
        application_kind: ApplicationKind,
        _redactor: &Redactor,
    ) -> bool {
        if !self.can_accept(command, run_id, turn_id, application_kind) {
            return false;
        }
        if self.commands.len() >= STEER_GROUP_MAX_COMMANDS {
            return false;
        }
        let Some(canonical) = command_plaintext_bytes(command) else {
            return false;
        };
        self.plaintext_bytes.saturating_add(canonical) <= STEER_GROUP_MAX_BYTES
    }

    pub(crate) fn push(&mut self, command: AdmittedCommand, _redactor: &Redactor) -> Result<()> {
        debug_assert!(matches!(
            command.envelope().command,
            Command::UserMessage { .. }
        ));
        let Some(canonical) = command_plaintext_bytes(&command) else {
            bail!("SteerGroup only accepts UserMessage commands");
        };
        if self.commands.len() >= STEER_GROUP_MAX_COMMANDS {
            bail!("steer group has reached the maximum command count");
        }
        if self.plaintext_bytes.saturating_add(canonical) > STEER_GROUP_MAX_BYTES {
            bail!("steer group would exceed the maximum plaintext size");
        }
        self.plaintext_bytes += canonical;
        self.commands.push(command);
        Ok(())
    }

    pub(crate) fn snapshot(
        self,
        previous_owner: DurableRunBinding,
        closing_turn_message: Option<PublicMessage>,
    ) -> SteerGroupSnapshot {
        SteerGroupSnapshot {
            application_kind: self.application_kind,
            run_id: self.run_id,
            turn_id: self.turn_id,
            previous_owner,
            commands: self.commands,
            closing_turn_message,
            closing_tool_results: Vec::new(),
        }
    }
}

/// Build the EventBatch that injects a soft or retry group.  For soft steer a
/// single `TurnStart` is emitted; for retry steer the group is injected mid-turn
/// and no `TurnStart` is produced.  Each member is injected in command-seq order;
/// the last member closes as the sole new owner.
pub(crate) fn steer_group_injection_batch(snapshot: SteerGroupSnapshot) -> Result<EventBatch> {
    let SteerGroupSnapshot {
        application_kind,
        run_id,
        turn_id,
        previous_owner,
        commands,
        closing_turn_message,
        closing_tool_results,
    } = snapshot;

    if commands.is_empty() {
        bail!("cannot inject an empty steer group");
    }

    let mode = match application_kind {
        ApplicationKind::SoftSteer => SteerMode::Soft,
        ApplicationKind::RetrySteer => SteerMode::Soft, // retry uses soft Steered events
        _ => bail!("unexpected application kind in steer group injection"),
    };

    let mut writes = Vec::new();

    // Soft steer closes the previous turn before starting the new one.
    if application_kind == ApplicationKind::SoftSteer {
        if let Some(message) = closing_turn_message {
            writes.push(EventWrite {
                event: Some(DurableEvent::turn_end(
                    &previous_owner.run_id,
                    &previous_owner.turn_id,
                    message,
                    closing_tool_results,
                )?),
                projections: Vec::new(),
            });
        } else {
            writes.push(EventWrite {
                event: Some(DurableEvent::empty_turn_end(
                    &previous_owner.run_id,
                    &previous_owner.turn_id,
                )?),
                projections: Vec::new(),
            });
        }
    }

    // Steered event per member, in seq order.
    for command in &commands {
        writes.push(EventWrite {
            event: Some(DurableEvent::steered(
                mode,
                command.envelope().command_id.to_string(),
                run_id.clone(),
                turn_id.clone(),
            )?),
            projections: vec![Projection::RunPhase {
                command_id: command.envelope().command_id.to_string(),
                run_id: run_id.clone(),
                expected: RunPhase::Classified,
                next: RunPhase::TurnStarted,
            }],
        });
    }

    // One TurnStart for soft; none for retry.
    if application_kind == ApplicationKind::SoftSteer {
        writes.push(EventWrite {
            event: Some(DurableEvent::turn_start(&run_id, &turn_id)?),
            projections: Vec::new(),
        });
    }

    // User MessageStart/End for each member, with sequential owner hand-off.
    let mut injected_commands = Vec::with_capacity(commands.len());
    let mut previous_owner_id = previous_owner.command_id.clone();
    let mut previous_owner_seq = previous_owner.command_seq;

    for command in &commands {
        let user_message = build_user_message(command)?;
        let user_message_id = crate::store::user_message_id(&command.envelope().command_id);
        let new_command_id = command.envelope().command_id.to_string();
        let new_command_seq = command.envelope().seq;

        // Close the previous owner/member and open this member in the same
        // transaction.  For the first member this closes the old run owner;
        // for later members it closes the previous group member.
        let start_projections = vec![
            Projection::CommandApplied {
                command_id: previous_owner_id.clone(),
                command_seq: previous_owner_seq,
                run_id: Some(run_id.clone()),
            },
            Projection::RunPhase {
                command_id: new_command_id.clone(),
                run_id: run_id.clone(),
                expected: RunPhase::TurnStarted,
                next: RunPhase::UserStarted,
            },
        ];

        writes.push(EventWrite {
            event: Some(DurableEvent::message(
                "message_start",
                &user_message_id,
                &user_message,
            )?),
            projections: start_projections,
        });

        let end_projections = vec![
            Projection::MessageEnd {
                message_id: user_message_id.clone(),
                role: "user",
                message: user_message.clone(),
                append_to_l0: true,
                provider_context: Vec::new(),
                eviction_footprint_tokens: 0,
            },
            Projection::RunPhase {
                command_id: new_command_id.clone(),
                run_id: run_id.clone(),
                expected: RunPhase::UserStarted,
                next: RunPhase::UserCommitted,
            },
        ];

        writes.push(EventWrite {
            event: Some(DurableEvent::message(
                "message_end",
                &user_message_id,
                &user_message,
            )?),
            projections: end_projections,
        });

        injected_commands.push(InjectedCommand::new(
            new_command_seq,
            command.envelope().command_id.clone(),
        ));

        previous_owner_id = new_command_id;
        previous_owner_seq = new_command_seq;
    }

    Ok(EventBatch {
        writes,
        injected_commands,
    })
}

pub(crate) fn build_user_message(command: &AdmittedCommand) -> Result<PublicMessage> {
    let Command::UserMessage { text, attachments } = &command.envelope().command else {
        bail!("steer group member is not a UserMessage");
    };
    if !attachments.is_empty() {
        bail!("T16 steer does not accept attachments");
    }
    Ok(PublicMessage::User(crate::provider::types::UserMessage {
        content: vec![crate::provider::types::UserContent::Text { text: text.clone() }],
        timestamp: command.received_at(),
    }))
}

// ---------------------------------------------------------------------------
// Sizing helpers
// ---------------------------------------------------------------------------

pub(crate) const STEER_GROUP_MAX_COMMANDS: usize = 16;
pub(crate) const STEER_GROUP_MAX_BYTES: usize = 1024 * 1024;

fn command_plaintext_bytes(command: &AdmittedCommand) -> Option<usize> {
    serde_json::to_vec(&command.envelope().command)
        .ok()
        .map(|bytes| bytes.len())
}

/// Bound a candidate group by command count and canonical plaintext bytes.
/// Returns the accepted prefix length and the total plaintext bytes.
pub(crate) fn bound_steer_group(
    candidates: &[&AdmittedCommand],
    _redactor: &Redactor,
) -> (usize, usize) {
    let mut total = 0usize;
    for (index, command) in candidates.iter().enumerate() {
        if index >= STEER_GROUP_MAX_COMMANDS {
            return (index, total);
        }
        let Some(canonical) = command_plaintext_bytes(command) else {
            return (index, total);
        };
        if total.saturating_add(canonical) > STEER_GROUP_MAX_BYTES {
            return (index, total);
        }
        total += canonical;
    }
    (candidates.len(), total)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, Utc};

    use super::*;
    use crate::{
        gateway::{Command, CommandEnvelope, CommandId, InboundCommand},
        provider::types::{
            PublicAssistantMessage, PublicMessage, StopReason, UserContent, UserMessage,
        },
        store::{
            ApplicationKind, DurableEvent, EventBatch, EventWriter, InjectedCommand, Projection,
            RunPhase, Store,
        },
    };

    fn test_timestamp() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-07-20T01:02:03.456789Z")
            .expect("valid test timestamp")
            .with_timezone(&Utc)
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

    async fn test_store() -> Arc<Store> {
        Store::session_test_store("steer-test")
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
    ) -> DateTime<Utc> {
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
        let binding = DurableRunBinding {
            command_id: command_id.to_owned(),
            command_seq: 1,
            run_id: run_id.to_owned(),
            turn_id: turn_id.to_owned(),
            executor_generation: crate::runtime::contracts::ProcessGeneration::MIN,
        };
        let assistant_message_id = format!("{}-assistant", command_id);

        // Persist the owner command and pin its durable receipt timestamp.
        let _ = persist_and_pin(store, writer, 1, command_id, "owner").await;

        // Classify as idle_run so the run exists.
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

        // Inject the owner to UserCommitted.
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

        // Transition from UserCommitted to the requested owner phase.
        let mut assistant_message: Option<(String, PublicMessage)> = None;
        if phase == RunPhase::AssistantStarted {
            let assistant = PublicMessage::Assistant(PublicAssistantMessage {
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

    #[test]
    fn classification_is_total_for_the_three_live_boundaries() {
        assert_eq!(
            SteerStage::AssistantGeneration.classify_user_command(),
            Some(ApplicationKind::HardSteer)
        );
        assert_eq!(
            SteerStage::ToolOrApproval.classify_user_command(),
            Some(ApplicationKind::SoftSteer)
        );
        assert_eq!(
            SteerStage::RetryWait.classify_user_command(),
            Some(ApplicationKind::RetrySteer)
        );
        assert_eq!(SteerStage::Other.classify_user_command(), None);
    }

    #[test]
    fn registered_attempt_is_cancelled_at_most_once() {
        let cancellation = Arc::new(AttemptCancellation::default());
        let token = CancellationToken::new();
        let _guard = cancellation
            .register(token.clone())
            .expect("register attempt");
        assert!(cancellation.has_registered_attempt());
        cancellation
            .reserve()
            .expect("reserve attempt")
            .cancel_after_commit();
        assert!(token.is_cancelled());
        assert!(!cancellation.has_registered_attempt());
        assert!(cancellation.reserve().is_err());
    }

    #[test]
    fn committed_registry_accepts_a_later_attempt_while_latch_remains_set() {
        let cancellation = Arc::new(AttemptCancellation::default());
        let first = CancellationToken::new();
        let _first_guard = cancellation
            .register(first.clone())
            .expect("register first attempt");
        cancellation
            .reserve()
            .expect("reserve first attempt")
            .cancel_after_commit();
        assert!(first.is_cancelled());
        assert!(cancellation.hard_steer_committed());

        let next = CancellationToken::new();
        let _next_guard = cancellation
            .register(next.clone())
            .expect("committed reservation must release registry ownership");
        assert!(cancellation.has_registered_attempt());
        assert!(cancellation.hard_steer_committed());
        cancellation
            .retire_committed()
            .expect("later attempt can retire normally");
        assert!(!next.is_cancelled());
    }

    #[test]
    fn normal_durable_retirement_leaves_token_unobserved() {
        let cancellation = Arc::new(AttemptCancellation::default());
        let token = CancellationToken::new();
        let _guard = cancellation
            .register(token.clone())
            .expect("register attempt");
        cancellation
            .retire_committed()
            .expect("retire committed attempt");
        assert!(!token.is_cancelled());
        assert!(cancellation.reserve().is_err());
    }

    #[test]
    fn missing_attempt_fails_before_the_commit_boundary() {
        let cancellation = Arc::new(AttemptCancellation::default());
        assert!(cancellation.reserve().is_err());
        assert!(!cancellation.hard_steer_committed());
    }

    #[test]
    fn failed_commit_reservation_restores_normal_retirement() {
        let cancellation = Arc::new(AttemptCancellation::default());
        let token = CancellationToken::new();
        let _guard = cancellation
            .register(token.clone())
            .expect("register attempt");
        cancellation
            .reserve()
            .expect("reserve attempt")
            .restore()
            .expect("restore failed commit reservation");
        assert!(cancellation.has_registered_attempt());
        cancellation
            .retire_committed()
            .expect("retire restored attempt");
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn hard_steer_step_zero_commits_classification_before_cancellation() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        // Old owner in AssistantStarted so the run has a live owner.
        let (owner, _owner_assistant_id) = owner_in_phase(
            &store,
            &writer,
            "00000000-0000-4000-8000-000000000001",
            "run-001",
            "turn-001",
            RunPhase::AssistantStarted,
        )
        .await;

        // Persist and pin the steering command.
        let _ = persist_and_pin(
            &store,
            &writer,
            2,
            "00000000-0000-4000-8000-000000000002",
            "steer now",
        )
        .await;

        let command = test_admitted(2, "00000000-0000-4000-8000-000000000002", "steer now");
        let turn_id = "turn-002";
        let batch = hard_steer_step_zero_batch(&owner, &command, turn_id).expect("build batch");

        // The batch must be projection-only (event: None) with CommandClassified
        // and a RunPhase transition from AssistantStarted to HardSteerRequested.
        assert_eq!(batch.writes.len(), 1);
        assert!(batch.writes[0].event.is_none());
        assert_eq!(batch.injected_commands.len(), 0);

        let mut has_classified = false;
        let mut has_run_phase = false;
        for projection in &batch.writes[0].projections {
            match projection {
                Projection::CommandClassified {
                    command_id,
                    application_kind,
                    run_id,
                    turn_id,
                } => {
                    has_classified = true;
                    assert_eq!(command_id, "00000000-0000-4000-8000-000000000002");
                    assert_eq!(*application_kind, ApplicationKind::HardSteer);
                    assert_eq!(run_id, "run-001");
                    assert_eq!(turn_id, "turn-002");
                }
                Projection::RunPhase {
                    command_id,
                    run_id,
                    expected,
                    next,
                } => {
                    has_run_phase = true;
                    assert_eq!(command_id, "00000000-0000-4000-8000-000000000001");
                    assert_eq!(run_id, "run-001");
                    assert_eq!(*expected, RunPhase::AssistantStarted);
                    assert_eq!(*next, RunPhase::HardSteerRequested);
                }
                _ => panic!("unexpected projection"),
            }
        }
        assert!(has_classified);
        assert!(has_run_phase);

        // Applying the batch advances the durable state.
        let seqs = writer
            .apply(batch)
            .await
            .expect("apply hard steer step zero");
        assert_eq!(seqs.len(), 0); // projection-only writes produce no event seqs

        let old_phase: String =
            sqlx::query_scalar("SELECT run_phase FROM inbound_commands WHERE command_id=?")
                .bind("00000000-0000-4000-8000-000000000001")
                .fetch_one(store.pool())
                .await
                .expect("read owner phase");
        assert_eq!(old_phase, "hard_steer_requested");

        let new_kind: String =
            sqlx::query_scalar("SELECT application_kind FROM inbound_commands WHERE command_id=?")
                .bind("00000000-0000-4000-8000-000000000002")
                .fetch_one(store.pool())
                .await
                .expect("read steer kind");
        assert_eq!(new_kind, "hard_steer");
    }

    #[tokio::test]
    async fn hard_steer_finalize_emits_exact_six_three_one_sequence() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        // Old owner in AssistantStarted, then apply hard-steer step zero so
        // the durable state matches the real Session flow.
        let (owner, owner_assistant) = owner_in_phase(
            &store,
            &writer,
            "00000000-0000-4000-8000-000000000003",
            "run-003",
            "turn-003",
            RunPhase::AssistantStarted,
        )
        .await;
        let owner_assistant_id = owner_assistant.as_ref().unwrap().0.clone();

        let _ = persist_and_pin(
            &store,
            &writer,
            2,
            "00000000-0000-4000-8000-000000000004",
            "finalize",
        )
        .await;
        let command = test_admitted(2, "00000000-0000-4000-8000-000000000004", "finalize");
        writer
            .apply(hard_steer_step_zero_batch(&owner, &command, "turn-004").expect("batch"))
            .await
            .expect("classify steer and move owner to hard_steer_requested");

        let partial = PublicMessage::Assistant(PublicAssistantMessage {
            content: vec![
                PublicAssistantContent::Text {
                    text: "partial".to_owned(),
                    wire_item_index: 0,
                },
                PublicAssistantContent::ToolCall {
                    tool_call: crate::provider::types::ToolCall {
                        id: "call-1".to_owned(),
                        name: "bash".to_owned(),
                        arguments: serde_json::from_value(serde_json::json!({})).unwrap(),
                    },
                    wire_item_index: 1,
                },
            ],
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
        });

        let batches =
            finalize_hard_steer_batches(&owner, &command, owner_assistant_id, partial, "turn-004")
                .expect("build finalize batches");
        assert_eq!(batches.len(), 2);

        // Concatenate the two batches to verify the §6.3.1 event type sequence.
        let kinds: Vec<&str> = batches
            .iter()
            .flat_map(|batch| &batch.writes)
            .map(|write| write.event.as_ref().map(|_| "event").unwrap_or("none"))
            .collect();
        assert_eq!(
            kinds,
            vec!["event", "event", "event", "event", "event", "event"]
        );

        // The partial assistant MessageEnd projection dropped the tool-call and
        // appended the interruption marker.
        let projection = batches[0].writes[0]
            .projections
            .first()
            .expect("partial MessageEnd projection");
        if let Projection::MessageEnd {
            role,
            message: PublicMessage::Assistant(assistant),
            ..
        } = projection
        {
            assert_eq!(*role, "assistant");
            assert!(assistant.interrupted);
            assert_eq!(assistant.stop_reason, StopReason::Aborted);
            assert_eq!(assistant.content.len(), 1);
            if let PublicAssistantContent::Text { text, .. } = &assistant.content[0] {
                assert!(
                    text.ends_with("[この応答はユーザーの割り込みにより中断された]"),
                    "marker appended to partial text"
                );
            } else {
                panic!("partial assistant kept a non-text block");
            }
        } else {
            panic!("first projection is not partial assistant MessageEnd");
        }

        // Apply and verify owner hand-off.
        for batch in batches {
            writer.apply(batch).await.expect("apply finalize batch");
        }

        let old_status: String =
            sqlx::query_scalar("SELECT status FROM inbound_commands WHERE command_id=?")
                .bind("00000000-0000-4000-8000-000000000003")
                .fetch_one(store.pool())
                .await
                .expect("read old owner status");
        assert_eq!(old_status, "applied");

        let new_phase: String =
            sqlx::query_scalar("SELECT run_phase FROM inbound_commands WHERE command_id=?")
                .bind("00000000-0000-4000-8000-000000000004")
                .fetch_one(store.pool())
                .await
                .expect("read new owner phase");
        assert_eq!(new_phase, "user_committed");

        let events: Vec<String> =
            sqlx::query_scalar("SELECT event_type FROM agent_events ORDER BY seq")
                .fetch_all(store.pool())
                .await
                .expect("read events");
        let expected_sequence = vec![
            "message_end",
            "turn_end",
            "steered",
            "turn_start",
            "message_start",
            "message_end",
        ];
        let matched = events.windows(expected_sequence.len()).any(|window| {
            window.iter().map(String::as_str).collect::<Vec<_>>() == expected_sequence
        });
        assert!(
            matched,
            "agent_events should contain the exact §6.3.1 sequence: {events:?}"
        );
    }

    #[tokio::test]
    async fn soft_steer_group_injection_transfers_ownership_to_last_member() {
        let store = test_store().await;
        let writer = EventWriter::new(store.clone());

        // Old owner in AssistantStarted (active tool/approval attempt).
        let (owner, owner_assistant) = owner_in_phase(
            &store,
            &writer,
            "00000000-0000-4000-8000-000000000005",
            "run-005",
            "turn-005",
            RunPhase::AssistantStarted,
        )
        .await;

        // The assistant message must be durably closed before the turn can end.
        let (owner_assistant_id, owner_assistant_message) =
            owner_assistant.expect("assistant message");
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::message_in_turn(
                            "message_end",
                            &owner_assistant_id,
                            &owner_assistant_message,
                            Some(owner.run_id.clone()),
                            Some(owner.turn_id.clone()),
                        )
                        .expect("assistant MessageEnd"),
                    ),
                    projections: vec![Projection::MessageEnd {
                        message_id: owner_assistant_id,
                        role: "assistant",
                        message: owner_assistant_message.clone(),
                        append_to_l0: true,
                        provider_context: Vec::new(),
                        eviction_footprint_tokens: 0,
                    }],
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("close assistant message");

        // Classify three steering commands as SoftSteer in the same run.
        let members = [
            (2, "00000000-0000-4000-8000-000000000006", "first"),
            (3, "00000000-0000-4000-8000-000000000007", "second"),
            (4, "00000000-0000-4000-8000-000000000008", "third"),
        ];
        let mut admitted = Vec::with_capacity(members.len());
        for (seq, command_id, text) in members {
            let _ = persist_and_pin(&store, &writer, seq, command_id, text).await;
            let command = test_admitted(seq, command_id, text);
            writer
                .apply(EventBatch {
                    writes: vec![EventWrite {
                        event: None,
                        projections: vec![Projection::CommandClassified {
                            command_id: command_id.to_owned(),
                            application_kind: ApplicationKind::SoftSteer,
                            run_id: "run-005".to_owned(),
                            turn_id: "turn-006".to_owned(),
                        }],
                    }],
                    injected_commands: Vec::new(),
                })
                .await
                .expect("classify soft steer");
            admitted.push(command);
        }

        let snapshot = SteerGroupSnapshot {
            application_kind: ApplicationKind::SoftSteer,
            run_id: "run-005".to_owned(),
            turn_id: "turn-006".to_owned(),
            previous_owner: owner,
            commands: admitted,
            closing_turn_message: Some(owner_assistant_message),
            closing_tool_results: Vec::new(),
        };
        let batch = steer_group_injection_batch(snapshot).expect("build soft group batch");

        // TurnEnd + three Steered events + one TurnStart + two events per
        // member = 1 + 3 + 1 + 6 = 11 writes.
        assert_eq!(batch.writes.len(), 11);

        writer.apply(batch).await.expect("apply soft group");

        let relevant: Vec<String> = sqlx::query_scalar(
            "SELECT event_type FROM agent_events
             WHERE event_type IN ('turn_end','steered','turn_start','message_start','message_end')
             ORDER BY seq",
        )
        .fetch_all(store.pool())
        .await
        .expect("read relevant events");
        assert_eq!(
            relevant,
            vec![
                "turn_start",
                "message_start",
                "message_end", // owner user injection
                "message_start",
                "message_end", // assistant message
                "turn_end",    // close previous turn
                "steered",
                "steered",
                "steered",
                "turn_start",
                "message_start",
                "message_end",
                "message_start",
                "message_end",
                "message_start",
                "message_end",
            ]
        );

        let owner_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbound_commands
             WHERE run_id='run-005' AND command_kind='user_message' AND status='applying'
               AND run_phase IN ('user_started','user_committed','assistant_started','hard_steer_requested','cancel_requested')",
        )
        .fetch_one(store.pool())
        .await
        .expect("count owners");
        assert_eq!(owner_count, 1, "only the last group member remains owner");

        let last_phase: String = sqlx::query_scalar(
            "SELECT run_phase FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000008'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read last member");
        assert_eq!(last_phase, "user_committed");

        let first_status: String = sqlx::query_scalar(
            "SELECT status FROM inbound_commands WHERE command_id='00000000-0000-4000-8000-000000000006'",
        )
        .fetch_one(store.pool())
        .await
        .expect("read first member");
        assert_eq!(first_status, "applied");
    }
}
