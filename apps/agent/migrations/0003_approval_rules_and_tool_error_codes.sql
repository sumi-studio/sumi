-- Add durable approval_rules and expand tool_executions error_code vocabulary
-- to cover approval_denied / approval_cancelled cleanup, preserving all
-- existing rows and constraints.
PRAGMA foreign_keys = OFF;

CREATE TABLE approval_rules (
  id TEXT NOT NULL PRIMARY KEY,
  tool TEXT NOT NULL,
  pattern TEXT NOT NULL,
  created_at TEXT NOT NULL
);

ALTER TABLE tool_executions RENAME TO tool_executions_old;

CREATE TABLE tool_executions (
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
      'length_guard', 'user_steer_cancelled', 'approval_denied', 'approval_cancelled'
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
      'length_guard', 'user_steer_cancelled', 'approval_denied', 'approval_cancelled'
    ))
  )
);

INSERT INTO tool_executions SELECT * FROM tool_executions_old;

DROP TABLE tool_executions_old;

PRAGMA foreign_keys = ON;
