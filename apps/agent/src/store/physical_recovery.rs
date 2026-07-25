//! T17 physical recovery receipt application ledger.
//!
//! This module owns the durable ledger that accepts `PhysicalRecoveryReceipt`
//! values from T27 and records them in the same transaction as the logical
//! suffix and `indeterminate` terminal events.  T17 does not fabricate the
//! physical proof; it only validates and idempotently applies receipts that
//! T27 has already persisted.

#![allow(dead_code)]

use std::collections::BTreeSet;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::Row;

use crate::runtime::contracts::{
    GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease,
};

use super::Store;

/// A T27 physical recovery intent for one running tool execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalRecoveryIntent {
    pub tool_call_id: String,
    pub command_id: String,
    pub run_id: String,
    pub executor_generation: ProcessGeneration,
    pub indeterminate_terminal_seq: u64,
}

/// A T27 physical recovery receipt, bound to a `ProcessGeneration` lease and a
/// `GenerationRecoveryFence`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalRecoveryReceipt {
    pub receipt_id: String,
    pub lease: ProcessGenerationLease,
    pub fence: GenerationRecoveryFence,
    pub intents: Vec<PhysicalRecoveryIntent>,
    pub logical_suffix_first_seq: u64,
    pub logical_suffix_last_seq: u64,
    pub digest: String,
}

impl PhysicalRecoveryReceipt {
    /// Canonical receipt digest over the receipt identity, lease/fence,
    /// suffix bounds, and the sorted exact intent set.
    pub(crate) fn canonical_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(b"sumi-physical-recovery-receipt/v1");
        hasher.update(self.receipt_id.as_bytes());
        hasher.update(self.lease.lease_id().as_bytes());
        hasher.update(self.lease.generation().as_i64().to_be_bytes());
        hasher.update(self.fence.fence_id().as_bytes());
        hasher.update(self.logical_suffix_first_seq.to_be_bytes());
        hasher.update(self.logical_suffix_last_seq.to_be_bytes());

        let mut sorted: Vec<&PhysicalRecoveryIntent> = self.intents.iter().collect();
        sorted.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));
        for intent in sorted {
            hasher.update(intent.tool_call_id.as_bytes());
            hasher.update(intent.command_id.as_bytes());
            hasher.update(intent.run_id.as_bytes());
            hasher.update(intent.executor_generation.as_i64().to_be_bytes());
            hasher.update(intent.indeterminate_terminal_seq.to_be_bytes());
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn validate(&self) -> Result<()> {
        if self.receipt_id.is_empty() {
            bail!("physical recovery receipt_id must not be empty");
        }
        if self.intents.is_empty() {
            bail!("physical recovery receipt must contain at least one intent");
        }
        if self.logical_suffix_last_seq < self.logical_suffix_first_seq {
            bail!("physical recovery suffix last_seq must not precede first_seq");
        }

        let ids: BTreeSet<_> = self.intents.iter().map(|i| &i.tool_call_id).collect();
        if ids.len() != self.intents.len() {
            bail!("physical recovery intents must have unique tool_call_id values");
        }

        let expected = self.canonical_digest();
        if self.digest != expected {
            bail!("physical recovery receipt digest does not match canonical intent set");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyReceiptOutcome {
    Applied,
    AlreadyApplied,
}

/// Transactional owner for applying physical recovery receipts.
pub(crate) struct PhysicalRecoveryApplier<'a> {
    store: &'a Store,
}

impl<'a> PhysicalRecoveryApplier<'a> {
    pub(crate) fn new(store: &'a Store) -> Self {
        Self { store }
    }

    /// Applies a `PhysicalRecoveryReceipt` exactly once.
    ///
    /// Validates the lease/fence binding, the digest, the referenced events and
    /// tool executions, and then records the ledger rows and transitions the
    /// affected tool executions to `indeterminate` in one transaction.  A
    /// re-injection with the same receipt_id, digest, lease, generation, fence,
    /// and exact intent set returns `AlreadyApplied`.
    pub(crate) async fn apply(
        &self,
        receipt: &PhysicalRecoveryReceipt,
    ) -> Result<ApplyReceiptOutcome> {
        receipt.validate()?;

        let lease = &receipt.lease;
        let fence = &receipt.fence;
        fence
            .validate_exact(lease, fence.fence_id())
            .map_err(|e| anyhow::anyhow!("physical recovery fence/lease mismatch: {e}"))?;
        if receipt.lease.generation() != fence.generation() {
            bail!("physical recovery receipt generation does not match fence");
        }

        let mut transaction = self.store.pool().begin().await?;

        let existing = sqlx::query(
            "SELECT receipt_digest, lease_id, generation, fence_id, intent_count,
                    logical_suffix_first_seq, logical_suffix_last_seq, applied_at
             FROM physical_recovery_receipt_applications
             WHERE receipt_id = ?",
        )
        .bind(&receipt.receipt_id)
        .fetch_optional(&mut *transaction)
        .await
        .context("failed to load existing physical recovery receipt")?;

        if let Some(row) = existing {
            let digest: String = row.try_get("receipt_digest")?;
            let lease_id: String = row.try_get("lease_id")?;
            let stored_generation: i64 = row.try_get("generation")?;
            let fence_id: String = row.try_get("fence_id")?;
            let intent_count: i64 = row.try_get("intent_count")?;
            let first: i64 = row.try_get("logical_suffix_first_seq")?;
            let last: i64 = row.try_get("logical_suffix_last_seq")?;
            let applied_at: String = row.try_get("applied_at")?;

            let matches = digest == receipt.digest
                && lease_id == receipt.lease.lease_id()
                && stored_generation == receipt.lease.generation().as_i64()
                && fence_id == receipt.fence.fence_id()
                && intent_count as usize == receipt.intents.len()
                && first as u64 == receipt.logical_suffix_first_seq
                && last as u64 == receipt.logical_suffix_last_seq
                && !applied_at.is_empty();
            if !matches {
                bail!("conflicting physical recovery receipt already exists");
            }

            let stored_intents: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
                "SELECT tool_call_id, command_id, run_id, executor_generation,
                        indeterminate_terminal_seq
                 FROM physical_recovery_receipt_intents
                 WHERE receipt_id = ?
                 ORDER BY tool_call_id",
            )
            .bind(&receipt.receipt_id)
            .fetch_all(&mut *transaction)
            .await
            .context("failed to load existing receipt intents")?;

            let expected_intents: Vec<_> = {
                let mut v = receipt.intents.clone();
                v.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));
                v.into_iter()
                    .map(|i| {
                        (
                            i.tool_call_id,
                            i.command_id,
                            i.run_id,
                            i.executor_generation.as_i64(),
                            i.indeterminate_terminal_seq as i64,
                        )
                    })
                    .collect()
            };

            if stored_intents != expected_intents {
                bail!("conflicting physical recovery receipt intent set");
            }

            transaction.commit().await?;
            return Ok(ApplyReceiptOutcome::AlreadyApplied);
        }

        // Validate that the logical suffix bounds reference real events.
        let first_exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_events WHERE seq = ?")
                .bind(i64::try_from(receipt.logical_suffix_first_seq).unwrap_or(i64::MAX))
                .fetch_one(&mut *transaction)
                .await
                .context("failed to validate logical suffix first_seq")?;

        let last_exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_events WHERE seq = ?")
                .bind(i64::try_from(receipt.logical_suffix_last_seq).unwrap_or(i64::MAX))
                .fetch_one(&mut *transaction)
                .await
                .context("failed to validate logical suffix last_seq")?;

        if first_exists == 0 || last_exists == 0 {
            bail!("physical recovery receipt references non-existent logical suffix events");
        }

        // Validate each intent and transition the tool execution to indeterminate.
        for intent in &receipt.intents {
            let terminal_exists: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM agent_events WHERE seq = ?")
                    .bind(i64::try_from(intent.indeterminate_terminal_seq).unwrap_or(i64::MAX))
                    .fetch_one(&mut *transaction)
                    .await
                    .context("failed to validate indeterminate terminal seq")?;
            if terminal_exists == 0 {
                bail!("physical recovery intent references non-existent terminal event");
            }

            let tool_row = sqlx::query("SELECT state FROM tool_executions WHERE tool_call_id = ?")
                .bind(&intent.tool_call_id)
                .fetch_optional(&mut *transaction)
                .await
                .context("failed to load tool execution for intent")?;
            let Some(row) = tool_row else {
                bail!("physical recovery intent references missing tool execution");
            };
            let state: String = row.try_get("state")?;
            if state != "running" && state != "indeterminate" {
                bail!(
                    "tool execution {} is not in running/indeterminate state for physical recovery",
                    intent.tool_call_id
                );
            }

            if state == "running" {
                sqlx::query(
                    "UPDATE tool_executions
                     SET state = 'indeterminate', finished_at = ?, error_code = 'indeterminate'
                     WHERE tool_call_id = ? AND state = 'running'",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(&intent.tool_call_id)
                .execute(&mut *transaction)
                .await
                .context("failed to mark tool execution indeterminate")?;
            }
        }

        sqlx::query(
            "INSERT INTO physical_recovery_receipt_applications(
                receipt_id, receipt_digest, lease_id, fence_id, generation, intent_count,
                logical_suffix_first_seq, logical_suffix_last_seq, applied_at
             ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&receipt.receipt_id)
        .bind(&receipt.digest)
        .bind(receipt.lease.lease_id())
        .bind(receipt.fence.fence_id())
        .bind(receipt.lease.generation().as_i64())
        .bind(i64::try_from(receipt.intents.len()).unwrap_or(i64::MAX))
        .bind(i64::try_from(receipt.logical_suffix_first_seq).unwrap_or(i64::MAX))
        .bind(i64::try_from(receipt.logical_suffix_last_seq).unwrap_or(i64::MAX))
        .bind(Utc::now().to_rfc3339())
        .execute(&mut *transaction)
        .await
        .context("failed to insert physical recovery receipt application")?;

        for intent in &receipt.intents {
            sqlx::query(
                "INSERT INTO physical_recovery_receipt_intents(
                    receipt_id, tool_call_id, command_id, run_id, executor_generation,
                    indeterminate_terminal_seq
                 ) VALUES(?, ?, ?, ?, ?, ?)",
            )
            .bind(&receipt.receipt_id)
            .bind(&intent.tool_call_id)
            .bind(&intent.command_id)
            .bind(&intent.run_id)
            .bind(intent.executor_generation.as_i64())
            .bind(i64::try_from(intent.indeterminate_terminal_seq).unwrap_or(i64::MAX))
            .execute(&mut *transaction)
            .await
            .context("failed to insert physical recovery receipt intent")?;
        }

        transaction.commit().await?;
        Ok(ApplyReceiptOutcome::Applied)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_trait::async_trait;

    use crate::runtime::contracts::{
        GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease,
    };
    use crate::store::{
        AgentScope, DataKeyPurpose, KeyProvider, Store,
        crypto::{DATA_KEY_BYTES, WrappingKey},
    };

    use super::*;

    struct TestKeyProvider {
        key: WrappingKey,
    }

    #[async_trait]
    impl KeyProvider for TestKeyProvider {
        async fn current_key(&self) -> Result<WrappingKey> {
            Ok(self.key.clone())
        }

        async fn key_by_id(&self, key_id: &str) -> Result<WrappingKey> {
            if key_id != self.key.key_id() {
                bail!("unknown test key");
            }
            Ok(self.key.clone())
        }
    }

    fn test_lease(generation: u64) -> ProcessGenerationLease {
        ProcessGenerationLease::new(
            ProcessGeneration::from_wire(generation).expect("valid generation"),
            "lease-1",
        )
        .expect("valid lease")
    }

    fn test_fence(lease: &ProcessGenerationLease) -> GenerationRecoveryFence {
        GenerationRecoveryFence::new(lease, "fence-1").expect("valid fence")
    }

    async fn test_store() -> Arc<Store> {
        Store::in_memory(
            AgentScope {
                tenant_id: "tenant-1".to_owned(),
                agent_id: "agent-1".to_owned(),
                conversation_id: "conversation-1".to_owned(),
            },
            Arc::new(TestKeyProvider {
                key: WrappingKey::new("test-wrap-v1", [0x53; DATA_KEY_BYTES]),
            }),
        )
        .await
        .expect("open test store")
        .into()
    }

    async fn seed_events_and_execution(store: &Store) -> (u64, u64, String) {
        // Minimal agent_events rows for FK targets.  We insert two plain rows
        // without valid ciphertext/HMAC because the physical recovery applier
        // only needs the seq to exist for FK validation in these tests.
        let first_seq = 10u64;
        let last_seq = 11u64;
        let terminal_seq = 12u64;

        let key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");

        for seq in [first_seq, last_seq, terminal_seq] {
            sqlx::query(
                "INSERT INTO agent_events(
                    seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                    envelope, redaction_version, created_at
                 ) VALUES(?, 'message_end', '{}', ?, X'00', '{}', 1, ?)",
            )
            .bind(i64::try_from(seq).unwrap_or(i64::MAX))
            .bind(&key.key_ref)
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .expect("seed event row");
        }

        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES(?, 'command-1', 'run-1', 0, 'running', 'idem-1', ?, NULL, NULL)",
        )
        .bind("tool-call-1")
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .expect("seed tool execution");

        (first_seq, last_seq, "tool-call-1".to_owned())
    }

    fn receipt(
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        first: u64,
        last: u64,
        tool_call_id: &str,
        terminal_seq: u64,
    ) -> PhysicalRecoveryReceipt {
        let mut receipt = PhysicalRecoveryReceipt {
            receipt_id: "receipt-1".to_owned(),
            lease: lease.clone(),
            fence: fence.clone(),
            intents: vec![PhysicalRecoveryIntent {
                tool_call_id: tool_call_id.to_owned(),
                command_id: "command-1".to_owned(),
                run_id: "run-1".to_owned(),
                executor_generation: lease.generation(),
                indeterminate_terminal_seq: terminal_seq,
            }],
            logical_suffix_first_seq: first,
            logical_suffix_last_seq: last,
            digest: String::new(),
        };
        receipt.digest = receipt.canonical_digest();
        receipt
    }

    #[tokio::test]
    async fn applies_receipt_and_idempotent_replay() {
        let store = test_store().await;
        let (first, last, tool_call_id) = seed_events_and_execution(&store).await;
        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let receipt = receipt(&lease, &fence, first, last, &tool_call_id, 12);

        let applier = PhysicalRecoveryApplier::new(&store);
        assert_eq!(
            applier.apply(&receipt).await.expect("apply receipt"),
            ApplyReceiptOutcome::Applied
        );

        let state: String =
            sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id = ?")
                .bind(&tool_call_id)
                .fetch_one(store.pool())
                .await
                .expect("read tool state");
        assert_eq!(state, "indeterminate");

        assert_eq!(
            applier.apply(&receipt).await.expect("replay receipt"),
            ApplyReceiptOutcome::AlreadyApplied
        );
    }

    #[tokio::test]
    async fn rejects_conflicting_receipt_id_with_different_digest() {
        let store = test_store().await;
        let (first, last, tool_call_id) = seed_events_and_execution(&store).await;
        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let mut r = receipt(&lease, &fence, first, last, &tool_call_id, 12);

        let applier = PhysicalRecoveryApplier::new(&store);
        applier.apply(&r).await.expect("apply receipt");

        r.digest = "bad-digest".to_owned();
        assert!(
            applier.apply(&r).await.is_err(),
            "receipt with same id but different digest must be rejected"
        );
    }

    #[tokio::test]
    async fn rejects_missing_tool_execution() {
        let store = test_store().await;
        // seed only events, not the tool execution
        let (_first, _last, _tool_call_id) = seed_events_and_execution(&store).await;
        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let r = receipt(&lease, &fence, 10, 11, "missing-tool", 12);

        let applier = PhysicalRecoveryApplier::new(&store);
        assert!(applier.apply(&r).await.is_err());
    }

    #[tokio::test]
    async fn rejects_missing_suffix_event() {
        let store = test_store().await;
        let (_first, _last, tool_call_id) = seed_events_and_execution(&store).await;
        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let r = receipt(&lease, &fence, 99, 99, &tool_call_id, 12);

        let applier = PhysicalRecoveryApplier::new(&store);
        assert!(applier.apply(&r).await.is_err());
    }
}
