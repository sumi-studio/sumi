//! Durable handoff receipts for operations waiting on a person's decision.

use serde::{Deserialize, Serialize};

/// This is a receipt for accepting an operation, never an execution result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingOperationReceipt {
    pub status: PendingOperationStatus,
    pub operation_id: String,
    pub executed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PendingOperationStatus {
    AwaitingApproval,
}

impl PendingOperationReceipt {
    pub fn new(operation_id: String) -> Self {
        Self {
            status: PendingOperationStatus::AwaitingApproval,
            operation_id,
            executed: false,
        }
    }

    pub fn from_result(result: &crate::provider::types::ToolResultMessage) -> Option<Self> {
        let receipt: Self = serde_json::from_value(result.details.clone()).ok()?;
        (!receipt.executed && !receipt.operation_id.is_empty() && !result.is_error)
            .then_some(receipt)
    }
}
