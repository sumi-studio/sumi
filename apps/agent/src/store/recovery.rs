use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use sqlx::Row;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::agent::AgentEvent;
use crate::gateway::Command;

use super::{
    ApplicationKind, DataKeyPurpose, EventBatch, EventWrite, EventWriter, Projection, RunPhase,
    Store,
    crypto::decrypt_content,
    event_log::{EVENT_DIGEST_BYTES, EventChainEntry, extend_event_chain, verify_event_head},
    event_writer::DurableEventMetadata,
    verify_keyed_digest,
};

const EVENT_EVIDENCE_PAGE_ROWS: i64 = 64;
const PENDING_COMMAND_MAX_COUNT: usize = 32;
const PENDING_COMMAND_MAX_BYTES: usize = 4 * 1024 * 1024;
const RECOVERY_GROUP_MAX_COMMANDS: usize = 16;
const RECOVERY_GROUP_MAX_BYTES: usize = 1024 * 1024;

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
    },
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
    /// Advances only the restart suffix owned by the T12 durability foundation.
    ///
    /// T15 owns run/turn emission and provider/tool execution. Those planned
    /// steps remain durable and must not prevent the gateway from reopening.
    pub(crate) async fn recover_t12_prefix(
        store: &Store,
        writer: &EventWriter,
    ) -> Result<Vec<RecoveryStep>> {
        // Authenticate the entire append-only history once before any recovery
        // mutation. Subsequent iterations only choose the next bounded action.
        durable_event_evidence(store, EventEvidence::default()).await?;
        loop {
            let steps = Self::plan_next_without_history_scan(store).await?;
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

    /// Plans only the next missing durable action for each pending command/group.
    /// It deliberately does not manufacture a fixed MessageEnd/TurnEnd/AgentEnd
    /// suffix; assistant/tool/approval/cancel recovery must inspect the durable
    /// events and phase-specific state at execution time.
    #[allow(
        dead_code,
        reason = "T15 consumes the bounded next-action recovery planner"
    )]
    pub(crate) async fn plan(store: &Store) -> Result<Vec<RecoveryStep>> {
        Self::plan_next_without_history_scan(store).await
    }

    async fn plan_next_without_history_scan(store: &Store) -> Result<Vec<RecoveryStep>> {
        validate_pending_window(store).await?;
        let Some(command) = next_pending_command(store).await? else {
            durable_event_evidence(store, EventEvidence::default()).await?;
            return Ok(Vec::new());
        };
        let events = durable_event_evidence(
            store,
            EventEvidence::required_for(std::slice::from_ref(&command))?,
        )
        .await?;
        let step = match command.phase {
            RunPhase::Received => {
                if command.command_kind == "user_message" {
                    RecoveryStep::Reclassify {
                        command_id: command.command_id.clone(),
                    }
                } else {
                    RecoveryStep::ApplyControl {
                        command_id: command.command_id.clone(),
                    }
                }
            }
            RunPhase::Classified => {
                let kind = required_kind(&command)?;
                let run_id = required(command.run_id.as_deref(), "run_id", &command)?;
                let turn_id = required(command.turn_id.as_deref(), "turn_id", &command)?;
                if kind == ApplicationKind::IdleRun {
                    RecoveryStep::EmitAgentStart {
                        command_id: command.command_id.clone(),
                        run_id: run_id.to_owned(),
                    }
                } else {
                    RecoveryStep::InjectStoredGroup {
                        run_id: run_id.to_owned(),
                        turn_id: turn_id.to_owned(),
                        application_kind: kind,
                        command_ids: load_bounded_group(
                            store,
                            run_id,
                            turn_id,
                            kind,
                            command.phase,
                        )
                        .await?,
                    }
                }
            }
            RunPhase::RunStarted => {
                let run_id = required(command.run_id.as_deref(), "run_id", &command)?;
                if !events.has("agent_start", Some(run_id), None) {
                    bail!(
                        "run_started command {} has no durable AgentStart evidence",
                        command.command_id
                    );
                }
                RecoveryStep::EmitTurnStart {
                    command_id: command.command_id.clone(),
                    run_id: run_id.to_owned(),
                    turn_id: required(command.turn_id.as_deref(), "turn_id", &command)?.to_owned(),
                }
            }
            RunPhase::TurnStarted => {
                let kind = required_kind(&command)?;
                let run_id = required(command.run_id.as_deref(), "run_id", &command)?;
                let turn_id = required(command.turn_id.as_deref(), "turn_id", &command)?;
                if kind != ApplicationKind::RetrySteer
                    && !events.has("turn_start", Some(run_id), Some(turn_id))
                {
                    bail!(
                        "turn_started command {} has no durable TurnStart evidence",
                        command.command_id
                    );
                }
                RecoveryStep::InjectStoredGroup {
                    run_id: run_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    application_kind: kind,
                    command_ids: load_bounded_group(store, run_id, turn_id, kind, command.phase)
                        .await?,
                }
            }
            RunPhase::UserStarted => RecoveryStep::EmitUserMessageEnd {
                command_id: command.command_id.clone(),
                run_id: required(command.run_id.as_deref(), "run_id", &command)?.to_owned(),
                turn_id: required(command.turn_id.as_deref(), "turn_id", &command)?.to_owned(),
            },
            RunPhase::UserCommitted => RecoveryStep::StartAssistant {
                command_id: command.command_id.clone(),
                run_id: required(command.run_id.as_deref(), "run_id", &command)?.to_owned(),
                turn_id: required(command.turn_id.as_deref(), "turn_id", &command)?.to_owned(),
            },
            RunPhase::AssistantStarted => RecoveryStep::ResumeAssistantFromDurableEvents {
                command_id: command.command_id.clone(),
                run_id: required(command.run_id.as_deref(), "run_id", &command)?.to_owned(),
                turn_id: required(command.turn_id.as_deref(), "turn_id", &command)?.to_owned(),
            },
            RunPhase::HardSteerRequested => RecoveryStep::ResumeHardSteerFromDurableEvents {
                command_id: command.command_id.clone(),
                run_id: required(command.run_id.as_deref(), "run_id", &command)?.to_owned(),
                turn_id: required(command.turn_id.as_deref(), "turn_id", &command)?.to_owned(),
            },
            RunPhase::CancelRequested => RecoveryStep::ResumeCancellationFromDurableEvents {
                command_id: command.command_id.clone(),
                run_id: required(command.run_id.as_deref(), "run_id", &command)?.to_owned(),
                turn_id: required(command.turn_id.as_deref(), "turn_id", &command)?.to_owned(),
            },
            RunPhase::Finished => {
                bail!(
                    "finished command {} must have terminal status",
                    command.command_id
                );
            }
        };
        Ok(vec![step])
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
    verify_keyed_digest(
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
    verify_keyed_digest(&key, &plaintext, &payload_hmac)
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
         FROM event_log_heads WHERE conversation_id=?",
    )
    .bind(&store.scope().conversation_id)
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
                    AgentEvent::MemoryMaintenance { .. }
                        | AgentEvent::MessageUpdate { .. }
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
mod tests {
    use std::sync::Arc;

    use anyhow::{Result, bail};

    use super::*;
    use crate::{
        gateway::{ApprovalDecision, Command, CommandEnvelope, InboundCommand},
        store::{
            AgentScope, DurableEvent, EventBatch, EventWrite, EventWriter, Projection,
            crypto::{DATA_KEY_BYTES, WrappingKey},
        },
    };

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
                tenant_id: "tenant".to_owned(),
                agent_id: "agent".to_owned(),
                conversation_id: "conversation".to_owned(),
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

    async fn persist_user(writer: &EventWriter, seq: u64, id: &str) {
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq,
                command_id: crate::gateway::CommandId::parse(id)
                    .expect("test command_id must be canonical"),
                command: Command::UserMessage {
                    text: id.to_owned(),
                    attachments: Vec::new(),
                },
            }))
            .await
            .expect("persist user");
    }

    async fn persist_run_started(store: &Store, writer: &EventWriter) {
        persist_user(writer, 1, "00000000-0000-4000-8000-000000000001").await;
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

    async fn persist_retry_events(writer: &EventWriter, count: usize) {
        let writes = (0..count)
            .map(|attempt| EventWrite {
                event: Some(
                    DurableEvent::new(&serde_json::json!({
                        "type":"retry_scheduled",
                        "attempt":u32::try_from(attempt + 1).expect("bounded fixture"),
                        "delay_ms":1,
                        "retry_at":"2026-07-20T00:00:00Z",
                        "error_message":"fixture"
                    }))
                    .expect("retry event"),
                ),
                projections: Vec::new(),
            })
            .collect();
        writer
            .apply(EventBatch {
                writes,
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist retry event history");
    }

    #[tokio::test]
    async fn authenticated_event_head_accepts_valid_history_across_page_boundary() {
        let (store, writer) = setup().await;
        persist_retry_events(&writer, EVENT_EVIDENCE_PAGE_ROWS as usize + 1).await;

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
            (EVENT_EVIDENCE_PAGE_ROWS + 1, EVENT_EVIDENCE_PAGE_ROWS + 1)
        );
    }

    #[tokio::test]
    async fn authenticated_event_head_rejects_middle_and_tail_deletion() {
        let (middle_store, middle_writer) = setup().await;
        persist_retry_events(&middle_writer, 4).await;
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
        persist_retry_events(&tail_writer, 4).await;
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
        persist_retry_events(&writer, 4).await;
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
    async fn t12_startup_recovery_classifies_idle_then_leaves_run_start_pending() {
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
            "T15-owned AgentStart must remain pending"
        );
    }

    #[tokio::test]
    async fn t12_startup_recovery_applies_idle_abort_cutoff_before_reclassifying() {
        let (store, writer) = setup().await;
        persist_user(&writer, 1, "00000000-0000-4000-8000-000000000001").await;
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 2,
                command_id: crate::gateway::CommandId::parse(
                    "00000000-0000-4000-8000-000000000002",
                )
                .expect("command ID"),
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
    async fn t12_startup_recovery_terminals_unknown_approval_as_durable_noop() {
        let (store, writer) = setup().await;
        writer
            .persist_inbound(&InboundCommand::Valid(CommandEnvelope {
                seq: 1,
                command_id: crate::gateway::CommandId::parse(
                    "00000000-0000-4000-8000-000000000001",
                )
                .expect("command ID"),
                command: Command::ApprovalDecision {
                    request_id: "unknown-request".to_owned(),
                    decision: ApprovalDecision::Deny,
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
    async fn no_pending_commands_still_require_authenticated_event_history() {
        let (store, writer) = setup().await;
        writer
            .apply(EventBatch {
                writes: vec![EventWrite {
                    event: Some(
                        DurableEvent::new(&serde_json::json!({
                            "type":"retry_scheduled",
                            "attempt":1,
                            "delay_ms":1,
                            "retry_at":"2026-07-20T00:00:00Z",
                            "error_message":"fixture"
                        }))
                        .expect("event"),
                    ),
                    projections: Vec::new(),
                }],
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist history without a pending command");
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
        persist_run_started(&store, &writer).await;
        let writes = (0..(EVENT_EVIDENCE_PAGE_ROWS as usize * 2 + 1))
            .map(|attempt| EventWrite {
                event: Some(
                    DurableEvent::new(&serde_json::json!({
                        "type":"retry_scheduled",
                        "attempt": u32::try_from(attempt + 1).expect("bounded fixture"),
                        "delay_ms":1,
                        "retry_at":"2026-07-20T00:00:00Z",
                        "error_message":"fixture"
                    }))
                    .expect("event"),
                ),
                projections: Vec::new(),
            })
            .collect();
        writer
            .apply(EventBatch {
                writes,
                injected_commands: Vec::new(),
            })
            .await
            .expect("persist multi-page history");
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
                        DurableEvent::new(&serde_json::json!({
                            "type":"retry_scheduled",
                            "attempt":1,
                            "delay_ms":1,
                            "retry_at":"2026-07-20T00:00:00Z",
                            "error_message":"fixture"
                        }))
                        .expect("second event"),
                    ),
                    projections: Vec::new(),
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
            .conversation_key(DataKeyPurpose::Transcript)
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
            tenant_id: "tenant".to_owned(),
            agent_id: "agent".to_owned(),
            conversation_id: "conversation".to_owned(),
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
