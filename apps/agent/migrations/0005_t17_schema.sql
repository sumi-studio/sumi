CREATE VIRTUAL TABLE messages_fts USING fts5(
  search_text, content='messages', content_rowid='rowid', tokenize='trigram'
);

INSERT INTO messages_fts(rowid, search_text)
SELECT rowid, search_text FROM messages;

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
  FOREIGN KEY(message_id, message_seq) REFERENCES messages(id, seq),
  FOREIGN KEY(key_ref) REFERENCES data_keys(key_ref)
);

CREATE UNIQUE INDEX idx_provider_context_active_native_window
ON provider_context(provider_instance_id, protocol, model, kind)
WHERE message_id IS NULL;

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

CREATE TABLE provider_context_replace_heads (
  scope_key TEXT NOT NULL PRIMARY KEY,
  max_config_generation INTEGER NOT NULL,
  max_window_ordinal INTEGER NOT NULL,
  latest_insert_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (max_config_generation >= 0),
  CHECK (max_window_ordinal >= 0)
);

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

CREATE TABLE memory_batch_messages (
  batch_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  ord INTEGER NOT NULL,
  PRIMARY KEY(batch_id, ord),
  UNIQUE(message_id),
  FOREIGN KEY(batch_id) REFERENCES memory_batches(id) ON DELETE CASCADE,
  FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE
);

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

CREATE TABLE approval_rules (
  id TEXT NOT NULL PRIMARY KEY,
  tool TEXT NOT NULL,
  pattern TEXT NOT NULL,
  created_at TEXT NOT NULL
);
