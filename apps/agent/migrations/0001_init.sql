CREATE TABLE agent_scope (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE TABLE data_keys (
  key_ref TEXT NOT NULL PRIMARY KEY,
  scope TEXT NOT NULL,
  purpose TEXT NOT NULL,
  conversation_id TEXT,
  algorithm TEXT NOT NULL,
  wrap_key_id TEXT NOT NULL,
  wrap_nonce BLOB,
  wrapped_key BLOB,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  destroyed_at TEXT,
  CHECK (scope IN ('conversation', 'agent')),
  CHECK (purpose IN (
    'transcript', 'event', 'memory_summary', 'provider_context',
    'command', 'mutation', 'artifact', 'workspace'
  )),
  CHECK (
    (scope = 'conversation'
      AND conversation_id IS NOT NULL
      AND purpose IN (
        'transcript', 'event', 'memory_summary', 'provider_context',
        'command', 'mutation', 'artifact'
      ))
    OR
    (scope = 'agent'
      AND conversation_id IS NULL
      AND purpose = 'workspace')
  ),
  CHECK (
    (state = 'active'
      AND wrapped_key IS NOT NULL
      AND wrap_nonce IS NOT NULL
      AND destroyed_at IS NULL)
    OR
    (state = 'destroyed'
      AND wrapped_key IS NULL
      AND wrap_nonce IS NULL
      AND destroyed_at IS NOT NULL)
  )
);

CREATE UNIQUE INDEX one_active_shared_data_key
ON data_keys(scope, purpose, COALESCE(conversation_id, ''))
WHERE state = 'active' AND purpose <> 'provider_context';

CREATE TABLE messages (
  id TEXT NOT NULL PRIMARY KEY,
  seq INTEGER NOT NULL UNIQUE CHECK (seq >= 0),
  role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'tool_result')),
  raw_key_ref TEXT NOT NULL REFERENCES data_keys(key_ref),
  raw_ciphertext BLOB NOT NULL,
  payload TEXT NOT NULL,
  search_text TEXT NOT NULL,
  redaction_version INTEGER NOT NULL CHECK (redaction_version >= 1),
  interrupted INTEGER NOT NULL DEFAULT 0 CHECK (interrupted IN (0, 1)),
  created_at TEXT NOT NULL,
  UNIQUE(id, seq)
);

CREATE VIRTUAL TABLE messages_fts USING fts5(
  search_text, content='messages', content_rowid='rowid'
);

CREATE TRIGGER messages_fts_insert
AFTER INSERT ON messages
BEGIN
  INSERT INTO messages_fts(rowid, search_text) VALUES (new.rowid, new.search_text);
END;

CREATE TRIGGER messages_fts_delete
AFTER DELETE ON messages
BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, search_text)
  VALUES ('delete', old.rowid, old.search_text);
END;

CREATE TRIGGER messages_fts_update
AFTER UPDATE OF search_text ON messages
BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, search_text)
  VALUES ('delete', old.rowid, old.search_text);
  INSERT INTO messages_fts(rowid, search_text) VALUES (new.rowid, new.search_text);
END;

-- provider が発行した reasoning/compaction 等。provider-context データ鍵で暗号化し transcript と分離。
CREATE TABLE provider_context (
  id TEXT NOT NULL PRIMARY KEY,
  message_id TEXT,
  message_seq INTEGER,
  wire_item_index INTEGER,
  item_ordinal INTEGER NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  provider_instance_id TEXT NOT NULL,
  protocol TEXT NOT NULL,
  model TEXT NOT NULL,
  kind TEXT NOT NULL,
  coverage_through_seq INTEGER,
  context_fingerprint TEXT,
  key_ref TEXT NOT NULL,
  ciphertext BLOB NOT NULL,
  eviction_tokens INTEGER NOT NULL DEFAULT 0,
  eviction_estimator_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  CHECK ((message_id IS NULL) = (message_seq IS NULL)),
  CHECK (eviction_tokens >= 0),
  CHECK (eviction_estimator_version >= 1),
  CHECK (message_id IS NOT NULL OR eviction_tokens = 0),
  UNIQUE(message_id, wire_item_index, item_ordinal),
  FOREIGN KEY(message_id, message_seq) REFERENCES messages(id, seq) ON DELETE CASCADE,
  FOREIGN KEY(key_ref) REFERENCES data_keys(key_ref)
);

CREATE TABLE agent_events (
  seq INTEGER PRIMARY KEY CHECK (seq >= 0),
  event_type TEXT NOT NULL,
  internal_metadata TEXT NOT NULL,
  raw_key_ref TEXT NOT NULL REFERENCES data_keys(key_ref),
  raw_ciphertext BLOB NOT NULL,
  envelope TEXT NOT NULL,
  redaction_version INTEGER NOT NULL CHECK (redaction_version >= 1),
  created_at TEXT NOT NULL
);

-- Lifecycle identities remain globally unique without replaying/scanning the
-- append-only event history on every EventWriter transaction.
CREATE UNIQUE INDEX one_agent_start_per_run
ON agent_events(json_extract(internal_metadata, '$.run_id'))
WHERE event_type = 'agent_start';

CREATE UNIQUE INDEX one_turn_start_per_run_turn
ON agent_events(
  json_extract(internal_metadata, '$.run_id'),
  json_extract(internal_metadata, '$.turn_id')
)
WHERE event_type = 'turn_start';

CREATE UNIQUE INDEX one_message_start_per_message
ON agent_events(json_extract(envelope, '$.message_id'))
WHERE event_type = 'message_start';

CREATE TABLE event_log_heads (
  conversation_id TEXT NOT NULL PRIMARY KEY REFERENCES agent_scope(conversation_id),
  last_seq INTEGER NOT NULL CHECK (last_seq >= 1),
  event_count INTEGER NOT NULL CHECK (event_count >= 1),
  chain_digest BLOB NOT NULL CHECK (length(chain_digest) = 32),
  key_ref TEXT NOT NULL REFERENCES data_keys(key_ref),
  head_hmac BLOB NOT NULL CHECK (length(head_hmac) = 32),
  updated_at TEXT NOT NULL,
  CHECK (event_count = last_seq)
);

CREATE TABLE inbound_commands (
  seq INTEGER PRIMARY KEY CHECK (seq >= 0),
  command_id TEXT NOT NULL UNIQUE,
  command_kind TEXT NOT NULL,
  payload_ciphertext BLOB,
  payload_key_ref TEXT REFERENCES data_keys(key_ref),
  payload_hmac BLOB,
  status TEXT NOT NULL,
  reject_reason TEXT,
  reject_actual_bytes INTEGER,
  application_kind TEXT,
  run_id TEXT,
  turn_id TEXT,
  run_phase TEXT NOT NULL,
  received_at TEXT NOT NULL,
  applied_at TEXT,
  CHECK (command_kind IN ('user_message', 'abort', 'approval_decision', 'invalid')),
  CHECK (status IN ('received', 'applying', 'applied', 'superseded', 'rejected')),
  CHECK (
    application_kind IS NULL
    OR application_kind IN ('idle_run', 'hard_steer', 'soft_steer', 'retry_steer')
  ),
  CHECK (run_phase IN (
    'received', 'classified', 'run_started', 'turn_started', 'user_started',
    'user_committed', 'assistant_started', 'hard_steer_requested',
    'cancel_requested', 'finished'
  )),
  CHECK (
    (status IN ('received', 'applying') AND applied_at IS NULL)
    OR
    (status IN ('applied', 'superseded', 'rejected') AND applied_at IS NOT NULL)
  ),
  CHECK (
    (status <> 'rejected'
      AND payload_ciphertext IS NOT NULL
      AND payload_key_ref IS NOT NULL
      AND payload_hmac IS NOT NULL
      AND reject_reason IS NULL
      AND reject_actual_bytes IS NULL)
    OR
    (status = 'rejected'
      AND reject_reason IN (
        'unknown_command', 'schema_violation', 'attachments_not_empty', 'oversized'
      )
      AND (
        (reject_reason = 'oversized'
          AND payload_ciphertext IS NULL
          AND payload_key_ref IS NOT NULL
          AND payload_hmac IS NOT NULL
          AND reject_actual_bytes > 1048576)
        OR
        (reject_reason <> 'oversized'
          AND payload_ciphertext IS NOT NULL
          AND payload_key_ref IS NOT NULL
          AND payload_hmac IS NOT NULL
          AND reject_actual_bytes IS NULL)
      ))
  ),
  CHECK (
    (command_kind = 'user_message'
      AND status = 'received'
      AND application_kind IS NULL
      AND run_id IS NULL
      AND turn_id IS NULL
      AND run_phase = 'received')
    OR
    (command_kind = 'user_message'
      AND status = 'applying'
      AND application_kind IS NOT NULL
      AND run_id IS NOT NULL
      AND turn_id IS NOT NULL
      AND run_phase IN (
        'classified', 'run_started', 'turn_started', 'user_started',
        'user_committed', 'assistant_started', 'hard_steer_requested',
        'cancel_requested'
      ))
    OR
    (command_kind = 'user_message'
      AND status = 'applied'
      AND application_kind IS NOT NULL
      AND run_id IS NOT NULL
      AND turn_id IS NOT NULL
      AND run_phase = 'finished')
    OR
    (command_kind = 'user_message'
      AND status = 'superseded'
      AND application_kind IN ('hard_steer', 'soft_steer', 'retry_steer')
      AND run_id IS NOT NULL
      AND turn_id IS NOT NULL
      AND run_phase IN ('classified', 'turn_started'))
    OR
    (command_kind = 'user_message'
      AND status = 'superseded'
      AND application_kind = 'idle_run'
      AND run_id IS NOT NULL
      AND turn_id IS NOT NULL
      AND run_phase IN ('classified', 'run_started', 'turn_started'))
    OR
    (command_kind = 'user_message'
      AND status = 'superseded'
      AND application_kind IS NULL
      AND run_id IS NULL
      AND turn_id IS NULL
      AND run_phase = 'received')
    OR
    (command_kind IN ('abort', 'approval_decision')
      AND status IN ('received', 'applied')
      AND application_kind IS NULL
      AND run_id IS NULL
      AND turn_id IS NULL
      AND run_phase = 'received')
    OR
    (command_kind = 'invalid'
      AND status = 'rejected'
      AND application_kind IS NULL
      AND run_id IS NULL
      AND turn_id IS NULL
      AND run_phase = 'received')
  )
);

CREATE UNIQUE INDEX one_live_run_owner
ON inbound_commands(run_id)
WHERE command_kind = 'user_message'
  AND status = 'applying'
  AND run_phase IN (
    'user_started', 'user_committed', 'assistant_started',
    'hard_steer_requested', 'cancel_requested'
  );

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
      'length_guard'
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
    (state = 'not_started' AND error_code = 'length_guard')
  )
);

CREATE TABLE approval_log (
  id TEXT NOT NULL PRIMARY KEY,
  tool_call_id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL,
  turn_id TEXT NOT NULL,
  state TEXT NOT NULL
    CHECK (state IN (
      'pending', 'approved_once', 'approved_always', 'denied', 'cancelled'
    )),
  request_projection TEXT NOT NULL,
  redaction_version INTEGER NOT NULL CHECK (redaction_version >= 1),
  created_at TEXT NOT NULL,
  decided_at TEXT,
  CHECK (
    (state = 'pending' AND decided_at IS NULL)
    OR
    (state <> 'pending' AND decided_at IS NOT NULL)
  )
);

-- event=Noneのprovider-context mutationをprepare→applyでexactly-onceにする内部intent/log。
CREATE TABLE provider_context_mutations (
  mutation_id TEXT NOT NULL PRIMARY KEY,
  state TEXT NOT NULL,
  intent_key_ref TEXT NOT NULL,
  intent_ciphertext BLOB NOT NULL,
  hmac_key_id TEXT NOT NULL,
  intent_hmac BLOB NOT NULL,
  prepared_at TEXT NOT NULL,
  finished_at TEXT,
  terminal_reason TEXT,
  CHECK (state IN ('prepared', 'applied', 'superseded')),
  CHECK (terminal_reason IS NULL OR terminal_reason IN (
    'already_satisfied', 'newer_replace', 'newer_config_generation'
  )),
  CHECK (
    (state = 'prepared' AND finished_at IS NULL AND terminal_reason IS NULL)
    OR
    (state = 'applied' AND finished_at IS NOT NULL
      AND (terminal_reason IS NULL OR terminal_reason = 'already_satisfied'))
    OR
    (state = 'superseded' AND finished_at IS NOT NULL
      AND terminal_reason IS NOT NULL
      AND terminal_reason IN ('newer_replace', 'newer_config_generation'))
  ),
  FOREIGN KEY(intent_key_ref) REFERENCES data_keys(key_ref)
);

-- native Replaceの単調性証拠。active provider_context rowが後で削除されても後退させない。
CREATE TABLE provider_context_replace_heads (
  scope_key TEXT NOT NULL PRIMARY KEY,
  max_config_generation INTEGER NOT NULL,
  max_window_ordinal INTEGER NOT NULL,
  latest_insert_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (max_config_generation >= 0),
  CHECK (max_window_ordinal >= 0)
);

-- メモリ層の現在形 (再起動復元用)
CREATE TABLE memory_batches (
  id TEXT NOT NULL PRIMARY KEY,
  layer INTEGER NOT NULL,
  ord INTEGER NOT NULL,
  batch_seq INTEGER NOT NULL,
  version INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL,
  est_tokens INTEGER NOT NULL,
  eviction_footprint_tokens INTEGER NOT NULL DEFAULT 0,
  summary_key_ref TEXT,
  summary_ciphertext BLOB,
  summary_projection TEXT,
  summary_redaction_version INTEGER,
  updated_at TEXT NOT NULL,
  UNIQUE(layer, batch_seq),
  CHECK (layer IN (0, 1, 2)),
  CHECK (state IN (
    'open', 'sealed', 'compacting', 'compact_failed',
    'compacted', 'promoted', 'dropped'
  )),
  CHECK (est_tokens >= 0),
  CHECK (eviction_footprint_tokens >= 0),
  CHECK (
    (summary_key_ref IS NULL AND summary_ciphertext IS NULL
      AND summary_projection IS NULL AND summary_redaction_version IS NULL)
    OR
    (summary_key_ref IS NOT NULL AND summary_ciphertext IS NOT NULL
      AND summary_projection IS NOT NULL AND summary_redaction_version IS NOT NULL)
  ),
  FOREIGN KEY(summary_key_ref) REFERENCES data_keys(key_ref)
);

-- first/last の範囲推測ではなく、append_to_l0 を含む正確な membership を保存する。
CREATE TABLE memory_batch_messages (
  batch_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  ord INTEGER NOT NULL,
  PRIMARY KEY(batch_id, ord),
  UNIQUE(message_id),
  FOREIGN KEY(batch_id) REFERENCES memory_batches(id) ON DELETE CASCADE,
  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
);

-- Compact / L1→L2 / L2統合の耐久ジョブ。mpsc は wake-up 通知にしか使わない。
CREATE TABLE memory_jobs (
  id TEXT NOT NULL PRIMARY KEY,
  kind TEXT NOT NULL,
  batch_seq INTEGER NOT NULL,
  source_ids TEXT NOT NULL,
  source_versions TEXT NOT NULL,
  status TEXT NOT NULL,
  lease_until TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  result_key_ref TEXT,
  result_ciphertext BLOB,
  result_projection TEXT,
  result_redaction_version INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(kind, batch_seq),
  CHECK (status IN ('pending', 'running', 'completed', 'applied', 'failed')),
  CHECK (
    (result_key_ref IS NULL AND result_ciphertext IS NULL
      AND result_projection IS NULL AND result_redaction_version IS NULL)
    OR
    (result_key_ref IS NOT NULL AND result_ciphertext IS NOT NULL
      AND result_projection IS NOT NULL AND result_redaction_version IS NOT NULL)
  ),
  FOREIGN KEY(result_key_ref) REFERENCES data_keys(key_ref)
);

CREATE TABLE memory_apply_cursors (
  kind TEXT NOT NULL PRIMARY KEY,
  next_batch_seq INTEGER NOT NULL
);

CREATE TABLE kv (
  key TEXT NOT NULL PRIMARY KEY,
  value TEXT NOT NULL
);

-- 平文・agent-scoped で conversation reset を生き残る。secret を含む rule は保存前に拒否する (§9.4)。
CREATE TABLE approval_rules (
  id TEXT NOT NULL PRIMARY KEY,
  tool TEXT NOT NULL,
  pattern TEXT NOT NULL,
  created_at TEXT NOT NULL
);
