//! Session-owned asynchronous notification assessment. Admission and authority
//! remain in the ordinary command path; a failed assessment delivers the source.
use super::*;
use crate::{provider::types::ParentContextSnapshot, runtime::contracts::IncomingSource};
use futures_util::FutureExt;

pub(super) const ASSESSMENT_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) struct ReflexCompletion {
    command: AdmittedCommand,
    epoch: u64,
    snapshot: Option<ParentContextSnapshot>,
    decision: reflex::ReflexDecision,
}

impl<G: Gateway + 'static> Session<G> {
    pub(super) async fn restore_reflex_notifications(&mut self) -> Result<(), SessionFailure> {
        for (command, _) in self.writer.store().pending_reflex_commands().await? {
            self.received_user_replays.insert(ReceivedUserCommand {
                command_id: command.command_id.as_str().to_owned(),
                seq: command.seq,
            });
            self.admit_and_route(InboundCommand::Valid(command)).await?;
        }
        Ok(())
    }

    pub(super) async fn begin_reflex(
        &mut self,
        command: AdmittedCommand,
    ) -> Result<bool, SessionFailure> {
        if !matches!(command.envelope().command, Command::ExternalEvent { .. })
            || matches!(
                command.envelope().provenance.source(),
                IncomingSource::ApprovalOperation(_)
            )
        {
            return Ok(false);
        }
        let id = command.envelope().command_id.as_str().to_owned();
        if self.reflex_pending.contains_key(&id) {
            return Ok(true);
        }
        if self.reflex_pending.len() >= PENDING_CONTROL_CAPACITY {
            return Ok(false);
        }
        let stored = self.writer.store().reflex_decision(&id).await?;
        let snapshot = if stored.is_some() {
            // A pre-restart hard recommendation is never authority to interrupt
            // a new request. Retain its interpretation and deliver softly.
            None
        } else if self.active.is_some() {
            self.worker.latest_reflex_snapshot()
        } else if let Some(core) = self.core.as_ref() {
            match self.worker.idle_reflex_snapshot(core).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    tracing::warn!(%error, "notification snapshot unavailable; delivering original event");
                    None
                }
            }
        } else {
            None
        };
        if stored.is_none() && snapshot.is_none() {
            return Ok(false);
        }
        let PublicMessage::User(event) = steer::build_user_message(&command)? else {
            unreachable!("external event must produce a user message")
        };
        let store = self.writer.store().clone();
        let worker = self.worker.clone();
        let epoch = self.reflex_epoch;
        let cancel = self.runtime_shutdown.child_token();
        self.reflex_pending.insert(id, command.clone());
        self.reflex_jobs.spawn(async move {
            let assessed = if let Some(record) = stored {
                Some(record)
            } else {
                let decision = if let Some(parent) = snapshot.as_ref() {
                    match tokio::time::timeout(ASSESSMENT_TIMEOUT, AssertUnwindSafe(worker.evaluate_reflex(
                        parent,
                        &event,
                        reflex::ReflexLimits { max_defer_ms: 60 * 60 * 1000 },
                        cancel.child_token(),
                    )).catch_unwind()).await {
                        Ok(Ok(Ok(decision))) => decision,
                        Ok(Ok(Err(error))) => {
                            tracing::warn!(%error, "notification assessment failed; delivering original event");
                            reflex::ReflexDecision::Soft { interpretation: None }
                        }
                        Ok(Err(_)) | Err(_) => reflex::ReflexDecision::Soft { interpretation: None },
                    }
                } else {
                    reflex::ReflexDecision::Soft { interpretation: None }
                };
                match store.record_reflex_decision(command.envelope(), decision).await {
                    Ok(record) => Some(record),
                    Err(error) => {
                        tracing::warn!(%error, "notification assessment persistence failed; delivering original event");
                        None
                    }
                }
            };
            let decision = if let Some(record) = assessed {
                let delay = record.ready_at_ms.saturating_sub(Utc::now().timestamp_millis());
                if delay > 0 {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(delay as u64)) => {},
                        _ = cancel.cancelled() => {},
                    }
                }
                record.decision
            } else {
                reflex::ReflexDecision::Soft { interpretation: None }
            };
            ReflexCompletion { command, epoch, snapshot, decision }
        });
        Ok(true)
    }

    pub(super) async fn finish_reflex(
        &mut self,
        result: Option<Result<ReflexCompletion, tokio::task::JoinError>>,
    ) -> Result<(), SessionFailure> {
        let completion = match result {
            Some(Ok(completion)) => completion,
            Some(Err(error)) => {
                tracing::warn!(%error, "notification task failed; preserving original delivery");
                self.reflex_jobs.abort_all();
                let mut pending: Vec<_> = self
                    .reflex_pending
                    .drain()
                    .map(|(_, command)| command)
                    .collect();
                pending.sort_by_key(|command| command.envelope().seq);
                for command in pending {
                    self.deliver_reflex(command, false).await?;
                }
                return Ok(());
            }
            None => return Ok(()),
        };
        if self
            .reflex_pending
            .remove(completion.command.envelope().command_id.as_str())
            .is_none()
        {
            return Ok(());
        }
        let hard = completion.epoch == self.reflex_epoch
            && matches!(completion.decision, reflex::ReflexDecision::Hard { .. })
            && completion.snapshot.as_ref().is_some_and(|snapshot| {
                self.worker.latest_reflex_snapshot().as_ref() == Some(snapshot)
            });
        self.deliver_reflex(completion.command, hard).await
    }

    pub(super) async fn enrich_reflex(
        &mut self,
        command: AdmittedCommand,
    ) -> Result<AdmittedCommand, SessionFailure> {
        if !matches!(command.envelope().command, Command::ExternalEvent { .. }) {
            return Ok(command);
        }
        let interpretation = self
            .writer
            .store()
            .reflex_decision(command.envelope().command_id.as_str())
            .await?
            .and_then(|record| record.decision.interpretation().map(str::to_owned));
        Ok(command.with_event_interpretation(interpretation))
    }

    async fn deliver_reflex(
        &mut self,
        command: AdmittedCommand,
        hard: bool,
    ) -> Result<(), SessionFailure> {
        // A human Abort may have terminally superseded this source while its
        // assessment was pending. Never revive a command after that cutoff.
        let pending: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM inbound_commands WHERE command_id=? AND seq=? AND status='received' AND run_phase='received')")
            .bind(command.envelope().command_id.as_str())
            .bind(i64::try_from(command.envelope().seq).map_err(anyhow::Error::from)?)
            .fetch_one(self.writer.store().pool()).await.map_err(anyhow::Error::from)?;
        if !pending {
            return Ok(());
        }
        let command = self.enrich_reflex(command).await?;
        if self.active.is_none() {
            return self.route_idle(command).await;
        }
        if !self
            .active
            .as_ref()
            .is_some_and(|active| active.bridge.same_output_audience(&command))
        {
            return self.defer_active_command(command.without_hard_steer());
        }
        if hard && self.route_hard_steer(command.clone()).await? {
            return Ok(());
        }
        // A fresh hard recommendation gets only the attempt above. Soft,
        // stale, failed, or deferred assessments must remain non-interrupting
        // when the ordinary queue is reclassified at a later lifecycle event.
        let command = command.without_hard_steer();
        if self.route_retry_wait_command(&command).await?
            || self.route_soft_steer(command.clone()).await?
        {
            return Ok(());
        }
        self.defer_active_command(command)
    }
}
