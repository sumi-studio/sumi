-- 0018: Preserve the Human's exact current-call decision separately from the
-- foundation's commit-time execution disposition. A call approved once may
-- still be rejected before start when reauthorization no longer validates it.
-- Foundation-owned route denials also retain their precise error class.

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
      'approval_rejected', 'policy_denied', 'policy_unavailable',
      'execution_review_blocked', 'escalation_review_blocked', 'process_restarted'
    )
  ),
  invocation_route TEXT
    CHECK (invocation_route IS NULL OR invocation_route IN ('normal', 'elevated')),
  authority_provenance TEXT
    CHECK (
      authority_provenance IS NULL
      OR authority_provenance IN (
        'agent_own', 'agent_own_with_human_consent', 'human_account_one_shot'
      )
    ),
  descriptor_digest TEXT,
  bound_evidence_digest TEXT,
  bound_invocation_key_ref TEXT,
  bound_invocation_ciphertext BLOB,
  authorization_evidence_key_ref TEXT,
  authorization_evidence_ciphertext BLOB,
  authorization_evidence_digest TEXT,
  denial_evidence_key_ref TEXT,
  denial_evidence_ciphertext BLOB,
  denial_evidence_digest TEXT,
  CHECK (
    state IN ('prepared', 'running', 'succeeded', 'failed', 'cancelled', 'indeterminate', 'not_started')
  ),
  CHECK (
    (state = 'prepared' AND started_at IS NULL AND finished_at IS NULL)
    OR
    (state = 'running' AND started_at IS NOT NULL AND finished_at IS NULL)
    OR
    (state IN ('succeeded', 'failed', 'indeterminate')
      AND started_at IS NOT NULL AND finished_at IS NOT NULL)
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
      'approval_rejected', 'policy_denied', 'policy_unavailable',
      'execution_review_blocked', 'escalation_review_blocked', 'process_restarted'
    ))
  ),
  CHECK (
    error_code IS NULL OR error_code <> 'process_restarted' OR state = 'not_started'
  )
);

INSERT INTO new_tool_executions(
  tool_call_id, command_id, run_id, executor_generation, state,
  idempotency_key, started_at, finished_at, error_code,
  invocation_route, authority_provenance, descriptor_digest,
  bound_evidence_digest, bound_invocation_key_ref, bound_invocation_ciphertext,
  authorization_evidence_key_ref, authorization_evidence_ciphertext,
  authorization_evidence_digest, denial_evidence_key_ref,
  denial_evidence_ciphertext, denial_evidence_digest
) SELECT
  tool_call_id, command_id, run_id, executor_generation, state,
  idempotency_key, started_at, finished_at, error_code,
  invocation_route, authority_provenance, descriptor_digest,
  bound_evidence_digest, bound_invocation_key_ref, bound_invocation_ciphertext,
  authorization_evidence_key_ref, authorization_evidence_ciphertext,
  authorization_evidence_digest, denial_evidence_key_ref,
  denial_evidence_ciphertext, denial_evidence_digest
FROM tool_executions;

CREATE UNIQUE INDEX new_tool_executions_attestation
ON new_tool_executions(tool_call_id, command_id, run_id, executor_generation);

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

CREATE TABLE new_approval_log (
  id TEXT NOT NULL PRIMARY KEY,
  tool_call_id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL,
  turn_id TEXT NOT NULL,
  state TEXT NOT NULL
    CHECK (state IN (
      'pending', 'approved_once', 'approved_always', 'denied', 'rejected', 'cancelled'
    )),
  request_projection TEXT NOT NULL,
  redaction_version INTEGER NOT NULL CHECK (redaction_version >= 1),
  created_at TEXT NOT NULL,
  decided_at TEXT,
  invocation_route TEXT
    CHECK (invocation_route IS NULL OR invocation_route = 'elevated'),
  descriptor_digest TEXT,
  bound_evidence_digest TEXT,
  bound_invocation_key_ref TEXT,
  bound_invocation_ciphertext BLOB,
  escalation_review_key_ref TEXT,
  escalation_review_ciphertext BLOB,
  escalation_review_digest TEXT,
  policy_snapshot_key_ref TEXT,
  policy_snapshot_ciphertext BLOB,
  policy_snapshot_digest TEXT,
  human_decision_key_ref TEXT,
  human_decision_ciphertext BLOB,
  human_decision_digest TEXT,
  CHECK (
    (state = 'pending' AND decided_at IS NULL)
    OR
    (state <> 'pending' AND decided_at IS NOT NULL)
  )
);

INSERT INTO new_approval_log(
  id, tool_call_id, run_id, turn_id, state, request_projection,
  redaction_version, created_at, decided_at, invocation_route,
  descriptor_digest, bound_evidence_digest, bound_invocation_key_ref,
  bound_invocation_ciphertext, escalation_review_key_ref,
  escalation_review_ciphertext, escalation_review_digest,
  policy_snapshot_key_ref, policy_snapshot_ciphertext, policy_snapshot_digest,
  human_decision_key_ref, human_decision_ciphertext, human_decision_digest
) SELECT
  id, tool_call_id, run_id, turn_id, state, request_projection,
  redaction_version, created_at, decided_at, invocation_route,
  descriptor_digest, bound_evidence_digest, bound_invocation_key_ref,
  bound_invocation_ciphertext, escalation_review_key_ref,
  escalation_review_ciphertext, escalation_review_digest,
  policy_snapshot_key_ref, policy_snapshot_ciphertext, policy_snapshot_digest,
  human_decision_key_ref, human_decision_ciphertext, human_decision_digest
FROM approval_log;

DROP TABLE approval_log;
ALTER TABLE new_approval_log RENAME TO approval_log;
