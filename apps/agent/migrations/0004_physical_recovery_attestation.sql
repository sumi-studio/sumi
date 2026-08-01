-- Bind every T17 application-ledger child to the immutable tool execution
-- attestation.  The physical proof store remains T27-owned; these constraints
-- prevent a receipt from being rebound to a different command/run/generation.
CREATE UNIQUE INDEX IF NOT EXISTS tool_executions_attestation
ON tool_executions(tool_call_id, command_id, run_id, executor_generation);

ALTER TABLE physical_recovery_receipt_intents
  RENAME TO physical_recovery_receipt_intents_legacy;

CREATE TABLE physical_recovery_receipt_intents (
  receipt_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  command_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  executor_generation INTEGER NOT NULL,
  indeterminate_terminal_seq INTEGER NOT NULL,
  PRIMARY KEY(receipt_id, tool_call_id),
  UNIQUE(tool_call_id),
  UNIQUE(indeterminate_terminal_seq),
  FOREIGN KEY(receipt_id) REFERENCES physical_recovery_receipt_applications(receipt_id),
  FOREIGN KEY(tool_call_id, command_id, run_id, executor_generation)
    REFERENCES tool_executions(tool_call_id, command_id, run_id, executor_generation),
  FOREIGN KEY(indeterminate_terminal_seq) REFERENCES agent_events(seq),
  CHECK (executor_generation >= 0),
  CHECK (indeterminate_terminal_seq > 0)
);

INSERT INTO physical_recovery_receipt_intents(
  receipt_id, tool_call_id, command_id, run_id, executor_generation,
  indeterminate_terminal_seq
)
SELECT receipt_id, tool_call_id, command_id, run_id, executor_generation,
       indeterminate_terminal_seq
FROM physical_recovery_receipt_intents_legacy;

DROP TABLE physical_recovery_receipt_intents_legacy;
