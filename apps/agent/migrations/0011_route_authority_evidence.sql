-- ADR 0013 binds the immutable invocation route, app-owned operation
-- evidence, reviewer/Human decision evidence, and resolved execution
-- authority to the durable lifecycle row before any external effect.
--
-- These columns stay nullable for historical pre-ADR rows and the explicit
-- legacy test-fixture runtime.  Production route-aware writes populate an
-- all-or-nothing shape which cold hydration authenticates before recovery.

ALTER TABLE tool_executions ADD COLUMN invocation_route TEXT
  CHECK (invocation_route IS NULL OR invocation_route IN ('normal', 'elevated'));
ALTER TABLE tool_executions ADD COLUMN authority_provenance TEXT
  CHECK (
    authority_provenance IS NULL
    OR authority_provenance IN (
      'agent_own', 'agent_own_with_human_consent', 'human_account_one_shot'
    )
  );
ALTER TABLE tool_executions ADD COLUMN descriptor_digest TEXT;
ALTER TABLE tool_executions ADD COLUMN bound_evidence_digest TEXT;
ALTER TABLE tool_executions ADD COLUMN bound_invocation_key_ref TEXT;
ALTER TABLE tool_executions ADD COLUMN bound_invocation_ciphertext BLOB;
ALTER TABLE tool_executions ADD COLUMN authorization_evidence_key_ref TEXT;
ALTER TABLE tool_executions ADD COLUMN authorization_evidence_ciphertext BLOB;
ALTER TABLE tool_executions ADD COLUMN authorization_evidence_digest TEXT;
ALTER TABLE tool_executions ADD COLUMN denial_evidence_key_ref TEXT;
ALTER TABLE tool_executions ADD COLUMN denial_evidence_ciphertext BLOB;
ALTER TABLE tool_executions ADD COLUMN denial_evidence_digest TEXT;

ALTER TABLE approval_log ADD COLUMN invocation_route TEXT
  CHECK (invocation_route IS NULL OR invocation_route = 'elevated');
ALTER TABLE approval_log ADD COLUMN descriptor_digest TEXT;
ALTER TABLE approval_log ADD COLUMN bound_evidence_digest TEXT;
ALTER TABLE approval_log ADD COLUMN bound_invocation_key_ref TEXT;
ALTER TABLE approval_log ADD COLUMN bound_invocation_ciphertext BLOB;
ALTER TABLE approval_log ADD COLUMN escalation_review_key_ref TEXT;
ALTER TABLE approval_log ADD COLUMN escalation_review_ciphertext BLOB;
ALTER TABLE approval_log ADD COLUMN escalation_review_digest TEXT;
ALTER TABLE approval_log ADD COLUMN policy_snapshot_key_ref TEXT;
ALTER TABLE approval_log ADD COLUMN policy_snapshot_ciphertext BLOB;
ALTER TABLE approval_log ADD COLUMN policy_snapshot_digest TEXT;
ALTER TABLE approval_log ADD COLUMN human_decision_key_ref TEXT;
ALTER TABLE approval_log ADD COLUMN human_decision_ciphertext BLOB;
ALTER TABLE approval_log ADD COLUMN human_decision_digest TEXT;
