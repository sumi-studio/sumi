-- Logical recovery may prove that a durably emitted ToolCall never reached
-- policy preparation or external execution.  Record that fact as an explicit
-- `not_started` ToolExecution without weakening the existing error vocabulary
-- or the physical-recovery attestation graph.

CREATE TABLE new_tool_executions (
  tool_call_id TEXT NOT NULL PRIMARY KEY,
  command_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  executor_generation INTEGER NOT NULL CHECK (executor_generation >= 0),
  state TEXT NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  started_at TEXT,
  finished_at TEXT,
  error_code TEXT CHECK (
    error_code IS NULL
    OR error_code IN (
      'executor_failed', 'cancelled', 'indeterminate', 'invalid_result', 'internal',
      'length_guard', 'user_steer_cancelled', 'approval_denied', 'approval_cancelled',
      'process_restarted'
    )
  ),
  CHECK (
    state IN ('prepared', 'running', 'succeeded', 'failed', 'cancelled', 'indeterminate', 'not_started')
  ),
  CHECK (
    (state = 'prepared' AND started_at IS NULL AND finished_at IS NULL)
    OR
    (state = 'running' AND started_at IS NOT NULL AND finished_at IS NULL)
    OR
    (state IN ('succeeded', 'failed', 'indeterminate')
      AND started_at IS NOT NULL
      AND finished_at IS NOT NULL)
    OR
    (state = 'cancelled' AND finished_at IS NOT NULL)
    OR
    (state = 'not_started' AND started_at IS NULL AND finished_at IS NOT NULL)
  ),
  CHECK (
    (state IN ('prepared', 'running', 'succeeded') AND error_code IS NULL)
    OR
    (state IN ('failed', 'cancelled', 'indeterminate') AND error_code IS NOT NULL)
    OR
    (state = 'not_started' AND error_code IN (
      'length_guard', 'user_steer_cancelled', 'approval_denied', 'approval_cancelled',
      'process_restarted'
    ))
  ),
  CHECK (
    error_code IS NULL OR error_code <> 'process_restarted' OR state = 'not_started'
  )
);

INSERT INTO new_tool_executions(
  tool_call_id, command_id, run_id, executor_generation, state,
  idempotency_key, started_at, finished_at, error_code
) SELECT
  tool_call_id, command_id, run_id, executor_generation, state,
  idempotency_key, started_at, finished_at, error_code
FROM tool_executions;

CREATE UNIQUE INDEX new_tool_executions_attestation
ON new_tool_executions(tool_call_id, command_id, run_id, executor_generation);

-- sqlx runs migrations inside a transaction, where toggling PRAGMA
-- foreign_keys is ineffective. Rebuild the attestation child first so the old
-- parent can be replaced while enforcement remains enabled.
ALTER TABLE physical_recovery_receipt_intents
  RENAME TO physical_recovery_receipt_intents_legacy;

CREATE TABLE new_physical_recovery_receipt_intents (
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
    REFERENCES new_tool_executions(tool_call_id, command_id, run_id, executor_generation),
  FOREIGN KEY(indeterminate_terminal_seq) REFERENCES agent_events(seq),
  CHECK (executor_generation >= 0),
  CHECK (indeterminate_terminal_seq > 0)
);

INSERT INTO new_physical_recovery_receipt_intents(
  receipt_id, tool_call_id, command_id, run_id, executor_generation,
  indeterminate_terminal_seq
)
SELECT receipt_id, tool_call_id, command_id, run_id, executor_generation,
       indeterminate_terminal_seq
FROM physical_recovery_receipt_intents_legacy;

DROP TABLE physical_recovery_receipt_intents_legacy;
DROP TABLE tool_executions;
ALTER TABLE new_tool_executions RENAME TO tool_executions;
CREATE UNIQUE INDEX tool_executions_attestation
ON tool_executions(tool_call_id, command_id, run_id, executor_generation);
DROP INDEX new_tool_executions_attestation;
ALTER TABLE new_physical_recovery_receipt_intents
  RENAME TO physical_recovery_receipt_intents;
