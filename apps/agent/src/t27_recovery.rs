//! T27 physical recovery receipt construction and application.
//!
//! This module closes the bootstrap-time loop for `Store::hydrate` returning
//! `HydrationOutcome::RecoveryRequired`: it translates a list of running-tool
//! intents into a canonical `PhysicalRecoveryReceipt`, builds the logical
//! terminal EventBatch, and applies it through `EventWriter` in one SQLite
//! transaction.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::provider::types::{PublicMessage, ToolResultMessage, UserContent};
use crate::runtime::contracts::{GenerationRecoveryFence, ProcessGenerationLease};
use crate::store::{
    EventBatch, EventWrite, EventWriter, PhysicalRecoveryIntent, PhysicalRecoveryIntentRequest,
    PhysicalRecoveryReceipt, Projection, ToolExecutionMutation,
};

const RECOVERED_TEXT: &str = "recovered";

/// Build and apply a `PhysicalRecoveryReceipt` for all running tool intents.
///
/// The receipt ID is deterministic from the recovery lease/fence and the sorted
/// canonical intent set, so a restart that replays the same recovery attempt
/// produces the same receipt identity.  Event sequence numbers are allocated
/// only inside the EventWriter gate by the `build` closure passed to
/// `EventWriter::apply_physical_recovery`.
pub(crate) async fn apply_physical_recovery_receipt(
    writer: &EventWriter,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    intents: Vec<PhysicalRecoveryIntentRequest>,
) -> Result<PhysicalRecoveryReceipt> {
    if intents.is_empty() {
        bail!("physical recovery requires at least one running tool intent");
    }

    let mut sorted_intents = intents;
    sorted_intents.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));
    let receipt_id = deterministic_receipt_id(lease, fence, &sorted_intents);

    let (outcome, _seqs, receipt) = writer
        .apply_physical_recovery(lease, fence, |next_seq| {
            let mut writes = Vec::with_capacity(sorted_intents.len() * 3 + 1);
            let mut receipt_intents = Vec::with_capacity(sorted_intents.len());
            let mut cursor = next_seq;

            for intent in &sorted_intents {
                let first_seq = cursor;
                if intent.tool_call_id.is_empty()
                    || intent.command_id.is_empty()
                    || intent.run_id.is_empty()
                    || intent.tool_name.is_empty()
                {
                    bail!("physical recovery intent identity and tool_name must not be empty");
                }

                writes.extend(tool_finish_writes(&intent.tool_call_id, &intent.tool_name));

                receipt_intents.push(PhysicalRecoveryIntent {
                    tool_call_id: intent.tool_call_id.clone(),
                    command_id: intent.command_id.clone(),
                    run_id: intent.run_id.clone(),
                    executor_generation: intent.executor_generation,
                    indeterminate_terminal_seq: first_seq,
                });

                cursor = cursor
                    .checked_add(3)
                    .context("durable event sequence overflow")?;
            }

            let logical_suffix_first_seq = next_seq;
            let logical_suffix_last_seq = cursor
                .checked_sub(1)
                .context("durable event sequence overflow")?;

            let mut receipt = PhysicalRecoveryReceipt {
                receipt_id,
                lease: lease.clone(),
                fence: fence.clone(),
                intents: receipt_intents,
                logical_suffix_first_seq,
                logical_suffix_last_seq,
                digest: String::new(),
            };
            receipt.digest = receipt.canonical_digest();

            writes.push(EventWrite {
                event: None,
                projections: vec![Projection::PhysicalRecovery(receipt.clone())],
            });

            Ok((
                EventBatch {
                    writes,
                    injected_commands: Vec::new(),
                },
                receipt,
            ))
        })
        .await
        .context("failed to apply physical recovery receipt through EventWriter")?;

    match outcome {
        crate::store::ApplyReceiptOutcome::Applied => {
            tracing::info!(
                receipt_id = %receipt.receipt_id,
                intents = receipt.intents.len(),
                first_seq = receipt.logical_suffix_first_seq,
                last_seq = receipt.logical_suffix_last_seq,
                "physical recovery receipt applied"
            );
        }
        crate::store::ApplyReceiptOutcome::AlreadyApplied => {
            tracing::info!(receipt_id = %receipt.receipt_id, "physical recovery receipt already applied");
        }
    }

    Ok(receipt)
}

fn deterministic_receipt_id(
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    intents: &[PhysicalRecoveryIntentRequest],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sumi-physical-recovery-receipt-id/v1");
    hasher.update(lease.lease_id().as_bytes());
    hasher.update(fence.fence_id().as_bytes());
    for intent in intents {
        hasher.update(intent.tool_call_id.as_bytes());
    }
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    format!("receipt-{encoded}")
}

fn tool_finish_writes(tool_call_id: &str, tool_name: &str) -> Vec<EventWrite> {
    let text = RECOVERED_TEXT;
    let result = PublicMessage::ToolResult(ToolResultMessage {
        tool_call_id: tool_call_id.to_owned(),
        tool_name: tool_name.to_owned(),
        content: vec![UserContent::Text {
            text: text.to_owned(),
        }],
        details: json!({ "text": text }),
        is_error: true,
        timestamp: Utc::now(),
    });
    let message_id = format!("{tool_call_id}-result");

    vec![
        EventWrite {
            event: Some(
                crate::store::DurableEvent::tool_execution_end(
                    tool_call_id.to_owned(),
                    serde_json::to_value(&result).expect("tool result serializes"),
                    true,
                    "indeterminate".to_owned(),
                    Some("indeterminate".to_owned()),
                )
                .expect("typed ToolExecutionEnd"),
            ),
            projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                tool_call_id: tool_call_id.to_owned(),
                expected: "running",
                state: "indeterminate",
                error_code: Some("indeterminate"),
            })],
        },
        EventWrite {
            event: Some(
                crate::store::DurableEvent::message("message_start", &message_id, &result)
                    .expect("tool result MessageStart"),
            ),
            projections: Vec::new(),
        },
        EventWrite {
            event: Some(
                crate::store::DurableEvent::message("message_end", &message_id, &result)
                    .expect("tool result MessageEnd"),
            ),
            projections: vec![Projection::MessageEnd {
                message_id,
                role: "tool_result",
                message: result,
                append_to_l0: true,
                provider_context: Vec::new(),
                eviction_footprint_tokens: 0,
            }],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::contracts::{
        GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease,
    };

    #[test]
    fn deterministic_receipt_id_is_stable_for_same_intents() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(1).unwrap(), "lease-1")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-1").unwrap();
        let intents = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-1".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(1).unwrap(),
        }];
        let id1 = deterministic_receipt_id(&lease, &fence, &intents);
        let id2 = deterministic_receipt_id(&lease, &fence, &intents);
        assert_eq!(id1, id2);
        assert!(id1.starts_with("receipt-"));
    }

    #[test]
    fn deterministic_receipt_id_changes_with_tool_call_id() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(1).unwrap(), "lease-1")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-1").unwrap();
        let intents_a = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-a".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(1).unwrap(),
        }];
        let intents_b = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-b".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(1).unwrap(),
        }];
        assert_ne!(
            deterministic_receipt_id(&lease, &fence, &intents_a),
            deterministic_receipt_id(&lease, &fence, &intents_b)
        );
    }

    #[test]
    fn tool_finish_writes_uses_intent_tool_name_not_bash_default() {
        // Recovery must synthesize a ToolResultMessage whose tool_name matches
        // the recovered intent, not a hard-coded "bash" fallback.
        let writes = tool_finish_writes("tool-1", "custom_tool");
        assert_eq!(writes.len(), 3);

        let message_end = writes
            .iter()
            .find_map(|write| match &write.projections[..] {
                [Projection::MessageEnd { message, .. }] => Some(message),
                _ => None,
            })
            .expect("message_end projection with the result message");
        match message_end {
            PublicMessage::ToolResult(result) => {
                assert_eq!(result.tool_name, "custom_tool");
                assert_eq!(result.tool_call_id, "tool-1");
                assert!(result.is_error);
            }
            _ => panic!("expected a tool result message"),
        }
    }
}
