//! Durable notification interpretation and delivery deadline.
//!
//! Original commands remain the sole source of authority and event content.
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use zeroize::Zeroizing;

use super::{
    DataKeyPurpose, Store,
    crypto::{decrypt_content, encrypt_content},
    event_writer,
};
use crate::{
    agent::reflex::ReflexDecision,
    gateway::{Command, CommandEnvelope, CommandId},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct StoredReflexDecision {
    pub decision: ReflexDecision,
    pub decided_at: DateTime<Utc>,
    pub ready_at_ms: i64,
}

impl Store {
    pub(crate) async fn record_reflex_decision(
        &self,
        command: &CommandEnvelope,
        decision: ReflexDecision,
    ) -> Result<StoredReflexDecision> {
        let mut transaction = self.pool().begin().await?;
        let original = event_writer::load_authenticated_command(
            self,
            &mut transaction,
            command.command_id.as_str(),
            command.seq,
            "user_message",
        )
        .await?;
        let provenance = event_writer::authenticated_command_provenance(
            self,
            &mut transaction,
            command.command_id.as_str(),
        )
        .await?;
        if original != command.command
            || provenance != command.provenance
            || !matches!(original, Command::ExternalEvent { .. })
            || !provenance.is_external()
        {
            bail!("notification interpretation must belong to its exact external command");
        }
        transaction.commit().await?;
        let now = Utc::now();
        let delay = match &decision {
            ReflexDecision::Defer { delay_ms, .. } => i64::try_from(*delay_ms)?,
            _ => 0,
        };
        let record = StoredReflexDecision {
            decision,
            decided_at: now,
            ready_at_ms: now
                .timestamp_millis()
                .checked_add(delay)
                .ok_or_else(|| anyhow::anyhow!("notification deadline overflows"))?,
        };
        let key = self.private_key(DataKeyPurpose::Command).await?;
        let aad = self.scope().row_aad(
            "notification_reflex",
            command.command_id.as_str(),
            DataKeyPurpose::Command,
        );
        let plaintext = Zeroizing::new(serde_json::to_vec(&record)?);
        let ciphertext = encrypt_content(&key, &plaintext, &aad)?;
        sqlx::query("INSERT INTO notification_reflex(command_id,decision_key_ref,decision_ciphertext,ready_at_ms) VALUES(?,?,?,?) ON CONFLICT(command_id) DO NOTHING")
            .bind(command.command_id.as_str()).bind(&key.key_ref).bind(ciphertext).bind(record.ready_at_ms).execute(self.pool()).await?;
        self.reflex_decision(command.command_id.as_str())
            .await?
            .ok_or_else(|| anyhow::anyhow!("notification interpretation disappeared"))
    }

    pub(crate) async fn reflex_decision(
        &self,
        command_id: &str,
    ) -> Result<Option<StoredReflexDecision>> {
        let mut transaction = self.pool().begin().await?;
        let record = self
            .reflex_decision_in_transaction(&mut transaction, command_id)
            .await?;
        transaction.commit().await?;
        Ok(record)
    }

    pub(crate) async fn reflex_decision_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        command_id: &str,
    ) -> Result<Option<StoredReflexDecision>> {
        let row = sqlx::query("SELECT decision_key_ref,decision_ciphertext,ready_at_ms FROM notification_reflex WHERE command_id=?").bind(command_id).fetch_optional(&mut **transaction).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let key = self
            .data_key_by_ref_in_transaction(transaction, row.try_get("decision_key_ref")?)
            .await?;
        let aad = self
            .scope()
            .row_aad("notification_reflex", command_id, DataKeyPurpose::Command);
        let ciphertext: Vec<u8> = row.try_get("decision_ciphertext")?;
        let plaintext = Zeroizing::new(decrypt_content(&key, &ciphertext, &aad)?);
        let record: StoredReflexDecision = serde_json::from_slice(&plaintext)?;
        if record.ready_at_ms != row.try_get::<i64, _>("ready_at_ms")? {
            bail!("notification deadline does not match authenticated decision");
        }
        Ok(Some(record))
    }

    pub(crate) async fn pending_reflex_commands(
        &self,
    ) -> Result<Vec<(CommandEnvelope, StoredReflexDecision)>> {
        let rows = sqlx::query("SELECT c.command_id,c.seq FROM inbound_commands c JOIN notification_reflex r ON r.command_id=c.command_id WHERE c.status='received' AND c.run_phase='received' ORDER BY c.seq").fetch_all(self.pool()).await?;
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            let id: String = row.try_get("command_id")?;
            let seq = u64::try_from(row.try_get::<i64, _>("seq")?)?;
            let mut transaction = self.pool().begin().await?;
            let command = event_writer::load_authenticated_command(
                self,
                &mut transaction,
                &id,
                seq,
                "user_message",
            )
            .await?;
            let provenance =
                event_writer::authenticated_command_provenance(self, &mut transaction, &id).await?;
            transaction.commit().await?;
            let record = self
                .reflex_decision(&id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("notification decision disappeared"))?;
            result.push((
                CommandEnvelope {
                    seq,
                    command_id: CommandId::parse(&id).map_err(anyhow::Error::msg)?,
                    personality_agent_id: self.scope().personality_agent_id.clone(),
                    provenance,
                    command,
                },
                record,
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        gateway::{InboundCommand, test_messaging_provenance, test_personality_agent_id},
        store::EventWriter,
    };
    use std::sync::Arc;

    fn notification() -> CommandEnvelope {
        CommandEnvelope {
            command_id: CommandId::parse("00000000-0000-4000-8000-000000000721").unwrap(),
            seq: 1,
            personality_agent_id: test_personality_agent_id(),
            provenance: test_messaging_provenance(),
            command: Command::ExternalEvent {
                content: "Original notification with its exact source".into(),
            },
        }
    }

    #[tokio::test]
    async fn deferred_notification_survives_reopen_with_original_source_and_deadline() {
        let dir =
            std::env::temp_dir().join(format!("sumi-reflex-journal-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("reflex.db");
        let command = notification();
        let decision = ReflexDecision::Defer {
            delay_ms: 60_000,
            interpretation: Some("A derived assessment, never replacement authority".into()),
        };
        let deadline;
        {
            let store = Arc::new(
                Store::session_test_file_store(&path, "reflex")
                    .await
                    .unwrap(),
            );
            EventWriter::new(store.clone())
                .persist_inbound(&InboundCommand::Valid(command.clone()))
                .await
                .unwrap();
            let record = store
                .record_reflex_decision(&command, decision.clone())
                .await
                .unwrap();
            deadline = record.ready_at_ms;
            assert_eq!(deadline - record.decided_at.timestamp_millis(), 60_000);
            let repeated = store
                .record_reflex_decision(
                    &command,
                    ReflexDecision::Hard {
                        interpretation: None,
                    },
                )
                .await
                .unwrap();
            assert_eq!(repeated.decision, decision);
            assert_eq!(repeated.ready_at_ms, deadline);
            let ciphertext: Vec<u8> =
                sqlx::query_scalar("SELECT decision_ciphertext FROM notification_reflex")
                    .fetch_one(store.pool())
                    .await
                    .unwrap();
            assert!(!String::from_utf8_lossy(&ciphertext).contains("derived assessment"));
            store.pool().close().await;
        }
        let store = Store::session_test_file_store(&path, "reflex")
            .await
            .unwrap();
        let pending = store.pending_reflex_commands().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, command);
        assert_eq!(pending[0].1.decision, decision);
        assert_eq!(pending[0].1.ready_at_ms, deadline);
        store.pool().close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn reflex_cannot_attach_to_rewritten_command_or_hide_tampered_deadline() {
        let store = Arc::new(
            Store::session_test_store("reflex-authentication")
                .await
                .unwrap(),
        );
        let command = notification();
        EventWriter::new(store.clone())
            .persist_inbound(&InboundCommand::Valid(command.clone()))
            .await
            .unwrap();
        let mut rewritten = command.clone();
        rewritten.command = Command::ExternalEvent {
            content: "Replacement command".into(),
        };
        assert!(
            store
                .record_reflex_decision(
                    &rewritten,
                    ReflexDecision::Soft {
                        interpretation: None
                    }
                )
                .await
                .is_err()
        );
        store
            .record_reflex_decision(
                &command,
                ReflexDecision::Defer {
                    delay_ms: 10,
                    interpretation: None,
                },
            )
            .await
            .unwrap();
        sqlx::query("UPDATE notification_reflex SET ready_at_ms=ready_at_ms+1")
            .execute(store.pool())
            .await
            .unwrap();
        assert!(store.pending_reflex_commands().await.is_err());
    }
}
