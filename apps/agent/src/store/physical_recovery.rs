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

fn sqlite_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite INTEGER range"))
}

fn sqlite_i64_usize(value: usize, field: &str) -> Result<i64> {
    sqlite_i64(value as u64, field)
}

/// A T27 physical recovery intent for one running tool execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalRecoveryIntent {
    pub tool_call_id: String,
    pub command_id: String,
    pub run_id: String,
    pub executor_generation: ProcessGeneration,
    pub indeterminate_terminal_seq: u64,
}

/// Typed request returned by T17 hydration before T27 performs physical
/// kill/reap.  It intentionally has no terminal sequence: that sequence is
/// allocated only by the EventWriter transaction that applies the receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PhysicalRecoveryIntentRequest {
    pub tool_call_id: String,
    pub command_id: String,
    pub run_id: String,
    pub executor_generation: ProcessGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HydrationReceiptIdentity {
    pub lease_id: String,
    pub generation: ProcessGeneration,
    pub fence_id: String,
    pub intent_count: usize,
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
        fn field(hasher: &mut Sha256, value: &[u8]) {
            hasher.update((value.len() as u64).to_be_bytes());
            hasher.update(value);
        }
        field(&mut hasher, b"sumi-physical-recovery-receipt/v1");
        field(&mut hasher, self.receipt_id.as_bytes());
        field(&mut hasher, self.lease.lease_id().as_bytes());
        hasher.update(self.lease.generation().as_i64().to_be_bytes());
        field(&mut hasher, self.fence.fence_id().as_bytes());
        hasher.update(self.logical_suffix_first_seq.to_be_bytes());
        hasher.update(self.logical_suffix_last_seq.to_be_bytes());

        let mut sorted: Vec<&PhysicalRecoveryIntent> = self.intents.iter().collect();
        sorted.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));
        for intent in sorted {
            field(&mut hasher, intent.tool_call_id.as_bytes());
            field(&mut hasher, intent.command_id.as_bytes());
            field(&mut hasher, intent.run_id.as_bytes());
            hasher.update(intent.executor_generation.as_i64().to_be_bytes());
            hasher.update(intent.indeterminate_terminal_seq.to_be_bytes());
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.receipt_id.is_empty() {
            bail!("physical recovery receipt_id must not be empty");
        }
        if self.intents.is_empty() {
            bail!("physical recovery receipt must contain at least one intent");
        }
        if self.logical_suffix_last_seq < self.logical_suffix_first_seq {
            bail!("physical recovery suffix last_seq must not precede first_seq");
        }

        // A receipt is proof for one fenced recovery lease.  The fence
        // carries the current owner identity; each intent's executor
        // generation remains the immutable attestation of the recovered
        // (possibly older) execution and is checked against its tool row.
        self.fence
            .validate_exact(&self.lease, self.fence.fence_id())
            .map_err(|error| anyhow::anyhow!("physical recovery fence/lease mismatch: {error}"))?;

        let ids: BTreeSet<_> = self.intents.iter().map(|i| &i.tool_call_id).collect();
        if ids.len() != self.intents.len() {
            bail!("physical recovery intents must have unique tool_call_id values");
        }

        for intent in &self.intents {
            if intent.tool_call_id.is_empty()
                || intent.command_id.is_empty()
                || intent.run_id.is_empty()
            {
                bail!("physical recovery intent identity must not be empty");
            }
            if intent.indeterminate_terminal_seq == 0 {
                bail!(
                    "physical recovery intent {} must reference a positive terminal sequence",
                    intent.tool_call_id
                );
            }
        }

        let expected = self.canonical_digest();
        if self.digest != expected {
            bail!("physical recovery receipt digest does not match canonical intent set");
        }
        Ok(())
    }

    /// Validate that this proof is bound to the lease/fence injected for the
    /// current hydration attempt.  `validate` only checks the receipt's
    /// internal self-consistency; this second boundary rejects stale proofs
    /// from a prior bootstrap even when they are otherwise well formed.
    pub(crate) fn validate_for(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
    ) -> Result<()> {
        self.validate()?;
        lease
            .validate_exact(self.lease.generation(), self.lease.lease_id())
            .map_err(|error| {
                anyhow::anyhow!("physical recovery receipt lease mismatch: {error}")
            })?;
        fence
            .validate_exact(lease, self.fence.fence_id())
            .map_err(|error| {
                anyhow::anyhow!("physical recovery receipt fence mismatch: {error}")
            })?;
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
    /// Legacy proof-ledger adapter used by migration fixtures. Production
    /// hydration must call `EventWriter::apply_physical_recovery`, which also
    /// couples logical terminal events/results to this ledger transaction.
    pub(crate) async fn apply(
        &self,
        receipt: &PhysicalRecoveryReceipt,
    ) -> Result<ApplyReceiptOutcome> {
        receipt.validate()?;

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

        self.validate_suffix(&mut transaction, receipt).await?;
        self.validate_exact_recovery_suffix(&mut transaction, receipt)
            .await?;

        // Validate each intent and transition the tool execution to indeterminate.
        for intent in &receipt.intents {
            self.validate_intent(&mut transaction, receipt, intent, true)
                .await?;

            let tool_row = sqlx::query(
                "SELECT state, command_id, run_id, executor_generation
                 FROM tool_executions WHERE tool_call_id = ?",
            )
            .bind(&intent.tool_call_id)
            .fetch_optional(&mut *transaction)
            .await
            .context("failed to load tool execution for intent")?;
            let Some(row) = tool_row else {
                bail!("physical recovery intent references missing tool execution");
            };
            let state: String = row.try_get("state")?;
            let command_id: String = row.try_get("command_id")?;
            let run_id: String = row.try_get("run_id")?;
            let generation: i64 = row.try_get("executor_generation")?;
            if command_id != intent.command_id
                || run_id != intent.run_id
                || generation != intent.executor_generation.as_i64()
            {
                bail!(
                    "physical recovery intent {} does not match immutable tool execution attestation",
                    intent.tool_call_id
                );
            }
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
        .bind(sqlite_i64_usize(
            receipt.intents.len(),
            "receipt intent_count",
        )?)
        .bind(sqlite_i64(
            receipt.logical_suffix_first_seq,
            "logical_suffix_first_seq",
        )?)
        .bind(sqlite_i64(
            receipt.logical_suffix_last_seq,
            "logical_suffix_last_seq",
        )?)
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
            .bind(sqlite_i64(
                intent.indeterminate_terminal_seq,
                "indeterminate_terminal_seq",
            )?)
            .execute(&mut *transaction)
            .await
            .context("failed to insert physical recovery receipt intent")?;
        }

        self.validate_ledger_count(&mut transaction, receipt)
            .await?;

        transaction.commit().await?;
        Ok(ApplyReceiptOutcome::Applied)
    }

    /// Apply the T17 ledger portion inside an EventWriter-owned transaction.
    /// All event rows and typed projections have already been inserted by the
    /// caller; this method only validates their exact relationship to the
    /// injected receipt and writes the parent/children atomically.
    pub(crate) async fn apply_in_transaction(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
        batch_event_seqs: &[u64],
    ) -> Result<ApplyReceiptOutcome> {
        receipt.validate()?;

        let existing = sqlx::query(
            "SELECT receipt_digest, lease_id, generation, fence_id, intent_count,
                    logical_suffix_first_seq, logical_suffix_last_seq, applied_at
             FROM physical_recovery_receipt_applications
             WHERE receipt_id = ?",
        )
        .bind(&receipt.receipt_id)
        .fetch_optional(&mut **transaction)
        .await?;
        if let Some(row) = existing {
            if !batch_event_seqs.is_empty() {
                bail!(
                    "already-applied physical recovery receipt must be replayed without logical writes"
                );
            }
            self.validate_existing_receipt(transaction, receipt, row)
                .await?;
            return Ok(ApplyReceiptOutcome::AlreadyApplied);
        }

        if batch_event_seqs.is_empty()
            || receipt.logical_suffix_first_seq != batch_event_seqs[0]
            || receipt.logical_suffix_last_seq
                != *batch_event_seqs.last().expect("non-empty event sequence")
        {
            bail!("physical recovery receipt does not cover the exact EventWriter suffix");
        }
        for pair in batch_event_seqs.windows(2) {
            if pair[1] != pair[0].saturating_add(1) {
                bail!("physical recovery logical suffix contains a sequence gap");
            }
        }
        self.validate_suffix(transaction, receipt).await?;
        self.validate_exact_recovery_suffix(transaction, receipt)
            .await?;
        for intent in &receipt.intents {
            self.validate_intent(transaction, receipt, intent, true)
                .await?;
            let state: String =
                sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id = ?")
                    .bind(&intent.tool_call_id)
                    .fetch_one(&mut **transaction)
                    .await?;
            if state != "indeterminate" {
                bail!(
                    "physical recovery intent {} requires EventWriter terminal tool mutation",
                    intent.tool_call_id
                );
            }
            let row = sqlx::query(
                "SELECT command_id, run_id, executor_generation
                 FROM tool_executions WHERE tool_call_id = ?",
            )
            .bind(&intent.tool_call_id)
            .fetch_one(&mut **transaction)
            .await?;
            let command_id: String = row.try_get("command_id")?;
            let run_id: String = row.try_get("run_id")?;
            let generation: i64 = row.try_get("executor_generation")?;
            if command_id != intent.command_id
                || run_id != intent.run_id
                || generation != intent.executor_generation.as_i64()
            {
                bail!(
                    "physical recovery intent {} does not match immutable tool execution attestation",
                    intent.tool_call_id
                );
            }
        }
        self.insert_ledger(transaction, receipt).await?;
        self.validate_ledger_count(transaction, receipt).await?;
        Ok(ApplyReceiptOutcome::Applied)
    }

    async fn validate_existing_receipt(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
        row: sqlx::sqlite::SqliteRow,
    ) -> Result<()> {
        let digest: String = row.try_get("receipt_digest")?;
        let lease_id: String = row.try_get("lease_id")?;
        let generation: i64 = row.try_get("generation")?;
        let fence_id: String = row.try_get("fence_id")?;
        let count: i64 = row.try_get("intent_count")?;
        let first: i64 = row.try_get("logical_suffix_first_seq")?;
        let last: i64 = row.try_get("logical_suffix_last_seq")?;
        if digest != receipt.digest
            || lease_id != receipt.lease.lease_id()
            || generation != receipt.lease.generation().as_i64()
            || fence_id != receipt.fence.fence_id()
            || count != sqlite_i64_usize(receipt.intents.len(), "receipt intent_count")?
            || first != sqlite_i64(receipt.logical_suffix_first_seq, "logical_suffix_first_seq")?
            || last != sqlite_i64(receipt.logical_suffix_last_seq, "logical_suffix_last_seq")?
        {
            bail!("conflicting physical recovery receipt already exists");
        }
        let stored: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
            "SELECT tool_call_id, command_id, run_id, executor_generation,
                    indeterminate_terminal_seq
             FROM physical_recovery_receipt_intents
             WHERE receipt_id = ? ORDER BY tool_call_id",
        )
        .bind(&receipt.receipt_id)
        .fetch_all(&mut **transaction)
        .await?;
        let mut expected = receipt.intents.clone();
        expected.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));
        let expected: Vec<_> = expected
            .into_iter()
            .map(|i| {
                (
                    i.tool_call_id,
                    i.command_id,
                    i.run_id,
                    i.executor_generation.as_i64(),
                    i.indeterminate_terminal_seq as i64,
                )
            })
            .collect();
        if stored != expected {
            bail!("conflicting physical recovery receipt intent set");
        }
        Ok(())
    }

    async fn insert_ledger(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
    ) -> Result<()> {
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
        .bind(sqlite_i64_usize(
            receipt.intents.len(),
            "receipt intent_count",
        )?)
        .bind(sqlite_i64(
            receipt.logical_suffix_first_seq,
            "logical_suffix_first_seq",
        )?)
        .bind(sqlite_i64(
            receipt.logical_suffix_last_seq,
            "logical_suffix_last_seq",
        )?)
        .bind(Utc::now().to_rfc3339())
        .execute(&mut **transaction)
        .await?;
        let mut intents = receipt.intents.clone();
        intents.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));
        for intent in intents {
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
            .bind(sqlite_i64(
                intent.indeterminate_terminal_seq,
                "indeterminate_terminal_seq",
            )?)
            .execute(&mut **transaction)
            .await?;
        }
        Ok(())
    }

    async fn validate_ledger_count(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
    ) -> Result<()> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM physical_recovery_receipt_intents WHERE receipt_id = ?",
        )
        .bind(&receipt.receipt_id)
        .fetch_one(&mut **transaction)
        .await?;
        if count != sqlite_i64_usize(receipt.intents.len(), "receipt intent_count")? {
            bail!("physical recovery receipt intent_count does not match child rows");
        }
        Ok(())
    }

    async fn validate_suffix(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
    ) -> Result<()> {
        let first = sqlite_i64(receipt.logical_suffix_first_seq, "logical_suffix_first_seq")?;
        let last = sqlite_i64(receipt.logical_suffix_last_seq, "logical_suffix_last_seq")?;
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM agent_events WHERE seq BETWEEN ? AND ?")
                .bind(first)
                .bind(last)
                .fetch_one(&mut **transaction)
                .await?;
        let expected = last
            .checked_sub(first)
            .and_then(|value| value.checked_add(1))
            .unwrap_or(0);
        if count != expected {
            bail!("physical recovery logical suffix is missing or contains a gap");
        }
        Ok(())
    }

    async fn validate_intent(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
        intent: &PhysicalRecoveryIntent,
        strict_terminal_event: bool,
    ) -> Result<()> {
        let query = if strict_terminal_event {
            "SELECT event_type, internal_metadata, envelope
             FROM agent_events WHERE seq = ? AND seq BETWEEN ? AND ?"
        } else {
            "SELECT event_type, internal_metadata, envelope
             FROM agent_events WHERE seq = ?"
        };
        let mut query = sqlx::query(query).bind(sqlite_i64(
            intent.indeterminate_terminal_seq,
            "indeterminate_terminal_seq",
        )?);
        if strict_terminal_event {
            query = query
                .bind(sqlite_i64(
                    receipt.logical_suffix_first_seq,
                    "logical_suffix_first_seq",
                )?)
                .bind(sqlite_i64(
                    receipt.logical_suffix_last_seq,
                    "logical_suffix_last_seq",
                )?);
        }
        let row = query
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "physical recovery intent references a terminal event outside the suffix"
                )
            })?;
        if !strict_terminal_event {
            return Ok(());
        }
        let event_type: String = row.try_get("event_type")?;
        let metadata: String = row.try_get("internal_metadata")?;
        let envelope: String = row.try_get("envelope")?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata)?;
        let envelope: serde_json::Value = serde_json::from_str(&envelope)?;
        if event_type != "tool_execution_end"
            || envelope.get("type").and_then(|value| value.as_str()) != Some("tool_execution_end")
            || envelope
                .get("tool_call_id")
                .and_then(|value| value.as_str())
                != Some(intent.tool_call_id.as_str())
            || metadata.get("tool_state").and_then(|value| value.as_str()) != Some("indeterminate")
            || metadata
                .get("tool_error_code")
                .and_then(|value| value.as_str())
                != Some("indeterminate")
        {
            bail!(
                "physical recovery terminal event for {} is not an indeterminate ToolExecutionEnd",
                intent.tool_call_id
            );
        }
        Ok(())
    }

    async fn validate_exact_recovery_suffix(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        receipt: &PhysicalRecoveryReceipt,
    ) -> Result<()> {
        let first = sqlite_i64(receipt.logical_suffix_first_seq, "logical_suffix_first_seq")?;
        let last = sqlite_i64(receipt.logical_suffix_last_seq, "logical_suffix_last_seq")?;
        let expected_tools: BTreeSet<&str> = receipt
            .intents
            .iter()
            .map(|intent| intent.tool_call_id.as_str())
            .collect();
        let rows = sqlx::query(
            "SELECT seq, event_type, envelope FROM agent_events
             WHERE seq BETWEEN ? AND ? ORDER BY seq",
        )
        .bind(first)
        .bind(last)
        .fetch_all(&mut **transaction)
        .await?;
        let mut starts = BTreeSet::<String>::new();
        let mut ends = BTreeSet::<String>::new();
        for row in rows {
            let seq: i64 = row.try_get("seq")?;
            let event_type: String = row.try_get("event_type")?;
            let envelope: serde_json::Value = serde_json::from_str(row.try_get("envelope")?)?;
            match event_type.as_str() {
                "tool_execution_end" => {
                    let tool = envelope
                        .get("tool_call_id")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            anyhow::anyhow!("recovery terminal event has no tool_call_id")
                        })?;
                    if !expected_tools.contains(tool)
                        || !receipt.intents.iter().any(|intent| {
                            intent.tool_call_id == tool
                                && intent.indeterminate_terminal_seq
                                    == u64::try_from(seq).unwrap_or_default()
                        })
                    {
                        bail!("recovery suffix contains an unrelated tool terminal event");
                    }
                }
                "message_start" | "message_end" => {
                    let message = envelope
                        .get("message")
                        .ok_or_else(|| anyhow::anyhow!("recovery message event has no message"))?;
                    if message.get("role").and_then(|value| value.as_str()) != Some("tool_result") {
                        bail!("recovery suffix contains a non-tool message event");
                    }
                    let tool = message
                        .get("tool_call_id")
                        .and_then(|value| value.as_str())
                        .ok_or_else(|| {
                            anyhow::anyhow!("recovery tool result has no tool_call_id")
                        })?;
                    if !expected_tools.contains(tool) {
                        bail!("recovery suffix contains a result for an unrelated tool");
                    }
                    if event_type == "message_start" {
                        starts.insert(tool.to_owned());
                    } else {
                        ends.insert(tool.to_owned());
                    }
                }
                "turn_end" | "agent_end" => {}
                _ => bail!("recovery suffix contains unrelated event type {event_type}"),
            }
        }
        if starts.len() != expected_tools.len() || ends.len() != expected_tools.len() {
            bail!("recovery suffix is missing a tool-result MessageStart/MessageEnd pair");
        }
        Ok(())
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
        // Minimal contiguous suffix with a typed indeterminate terminal event.
        let first_seq = 10u64;
        let last_seq = 12u64;
        let terminal_seq = 12u64;

        let key = store
            .conversation_key(DataKeyPurpose::Event)
            .await
            .expect("mint event key");

        for (seq, event_type, envelope) in [
            (
                first_seq,
                "message_start",
                r#"{"type":"message_start","message":{"role":"tool_result","tool_call_id":"tool-call-1"}}"#,
            ),
            (
                11,
                "message_end",
                r#"{"type":"message_end","message":{"role":"tool_result","tool_call_id":"tool-call-1"}}"#,
            ),
        ] {
            sqlx::query(
                "INSERT INTO agent_events(
                    seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                    envelope, redaction_version, created_at
                 ) VALUES(?, ?, '{}', ?, X'00', ?, 1, ?)",
            )
            .bind(sqlite_i64(seq, "seed event seq").unwrap())
            .bind(event_type)
            .bind(&key.key_ref)
            .bind(envelope)
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .expect("seed event row");
        }
        sqlx::query(
            "INSERT INTO agent_events(
                seq, event_type, internal_metadata, raw_key_ref, raw_ciphertext,
                envelope, redaction_version, created_at
             ) VALUES(?, 'tool_execution_end', ?, ?, X'00', ?, 1, ?)",
        )
        .bind(sqlite_i64(terminal_seq, "terminal event seq").unwrap())
        .bind(r#"{"tool_state":"indeterminate","tool_error_code":"indeterminate"}"#)
        .bind(&key.key_ref)
        .bind(r#"{"type":"tool_execution_end","tool_call_id":"tool-call-1","result":{},"is_error":true}"#)
        .bind(Utc::now().to_rfc3339())
        .execute(store.pool())
        .await
        .expect("seed terminal event row");

        sqlx::query(
            "INSERT INTO tool_executions(
                tool_call_id, command_id, run_id, executor_generation, state,
                idempotency_key, started_at, finished_at, error_code
             ) VALUES(?, 'command-1', 'run-1', 1, 'running', 'idem-1', ?, NULL, NULL)",
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
    async fn rejects_receipt_bound_to_a_stale_hydration_lease() {
        let store = test_store().await;
        let (first, last, tool_call_id) = seed_events_and_execution(&store).await;
        let old_lease = test_lease(1);
        let old_fence = test_fence(&old_lease);
        let receipt = receipt(&old_lease, &old_fence, first, last, &tool_call_id, 12);

        let current_lease = test_lease(2);
        let current_fence = test_fence(&current_lease);
        assert!(
            receipt
                .validate_for(&current_lease, &current_fence)
                .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_missing_tool_execution() {
        let store = test_store().await;
        let (first, last, tool_call_id) = seed_events_and_execution(&store).await;
        sqlx::query("DELETE FROM tool_executions WHERE tool_call_id = ?")
            .bind(&tool_call_id)
            .execute(store.pool())
            .await
            .expect("remove tool execution row");

        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let r = receipt(&lease, &fence, first, last, &tool_call_id, 12);

        let applier = PhysicalRecoveryApplier::new(&store);
        let error = applier.apply(&r).await.expect_err("missing tool execution");
        assert!(error.to_string().contains("tool execution"));
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

    #[tokio::test]
    async fn rejects_wrong_attestation_without_ghost_ledger_rows() {
        let store = test_store().await;
        let (first, last, tool_call_id) = seed_events_and_execution(&store).await;
        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let mut r = receipt(&lease, &fence, first, last, &tool_call_id, 12);
        r.intents[0].command_id = "other-command".to_owned();
        r.digest = r.canonical_digest();

        let applier = PhysicalRecoveryApplier::new(&store);
        assert!(applier.apply(&r).await.is_err());
        let state: String =
            sqlx::query_scalar("SELECT state FROM tool_executions WHERE tool_call_id = ?")
                .bind(&tool_call_id)
                .fetch_one(store.pool())
                .await
                .expect("read rolled-back tool state");
        assert_eq!(state, "running");
        let ledger_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM physical_recovery_receipt_applications")
                .fetch_one(store.pool())
                .await
                .expect("read rolled-back ledger");
        assert_eq!(ledger_rows, 0);
    }

    #[tokio::test]
    async fn empty_hydration_returns_only_fenced_receipt_identity() {
        let store = test_store().await;
        let lease = test_lease(1);
        let fence = test_fence(&lease);
        let (intents, receipt) = store
            .hydrate_recovery_intents(&lease, &fence)
            .await
            .expect("hydrate clean store");
        assert!(intents.is_empty());
        assert_eq!(receipt.expect("clean hydration receipt").intent_count, 0);
    }

    #[tokio::test]
    async fn hydration_preserves_old_executor_generation_for_reap_attestation() {
        let store = test_store().await;
        let (_first, _last, tool_call_id) = seed_events_and_execution(&store).await;
        let lease = test_lease(2);
        let fence = test_fence(&lease);
        let (intents, receipt) = store
            .hydrate_recovery_intents(&lease, &fence)
            .await
            .expect("hydrate old-generation running execution");
        assert!(receipt.is_none());
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].tool_call_id, tool_call_id);
        assert_eq!(intents[0].executor_generation, test_lease(1).generation());
    }
}
