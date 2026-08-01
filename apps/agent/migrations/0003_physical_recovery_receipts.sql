-- T27 physical proof store とは別の T17 application ledger。
-- 同一 receipt_id + digest + lease + generation + canonical exact intent set の
-- 再送だけを already-applied として受理する。
CREATE TABLE physical_recovery_receipt_applications (
  receipt_id TEXT NOT NULL PRIMARY KEY,
  receipt_digest TEXT NOT NULL,
  personality_agent_id TEXT NOT NULL REFERENCES agent_scope(personality_agent_id)
    CHECK (sumi_is_canonical_uuid_v7(personality_agent_id) = 1),
  lease_id TEXT NOT NULL,
  fence_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  intent_count INTEGER NOT NULL,
  logical_suffix_first_seq INTEGER NOT NULL,
  logical_suffix_last_seq INTEGER NOT NULL,
  applied_at TEXT NOT NULL,
  CHECK (intent_count > 0),
  CHECK (generation >= 0),
  CHECK (logical_suffix_first_seq >= 0),
  CHECK (logical_suffix_last_seq >= logical_suffix_first_seq),
  FOREIGN KEY(logical_suffix_first_seq) REFERENCES agent_events(seq),
  FOREIGN KEY(logical_suffix_last_seq) REFERENCES agent_events(seq)
);

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
  FOREIGN KEY(tool_call_id) REFERENCES tool_executions(tool_call_id),
  FOREIGN KEY(indeterminate_terminal_seq) REFERENCES agent_events(seq),
  CHECK (executor_generation >= 0)
);
