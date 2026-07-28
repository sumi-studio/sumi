-- Memory projection authentication is a prelaunch schema boundary. There is
-- deliberately no synthetic baseline for legacy rows: an upgrade containing
-- any pre-commitment memory state fails closed.
CREATE TABLE memory_projection_upgrade_guard (
  row_count INTEGER NOT NULL CHECK (row_count = 0)
);

INSERT INTO memory_projection_upgrade_guard(row_count)
SELECT
  (SELECT COUNT(*) FROM memory_batches) +
  (SELECT COUNT(*) FROM memory_batch_messages) +
  (SELECT COUNT(*) FROM memory_jobs) +
  (SELECT COUNT(*) FROM memory_apply_cursors) +
  (SELECT COUNT(*) FROM kv WHERE key = 'calib.ratio') +
  (SELECT COUNT(*) FROM provider_context) +
  (SELECT COUNT(*) FROM provider_context_mutations) +
  (SELECT COUNT(*) FROM provider_context_replace_heads);

DROP TABLE memory_projection_upgrade_guard;

DROP TABLE memory_batch_messages;
DROP TABLE memory_batches;
DROP TABLE memory_jobs;
DROP TABLE memory_apply_cursors;

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
  membership_count INTEGER NOT NULL DEFAULT 0,
  membership_digest BLOB NOT NULL DEFAULT X'0000000000000000000000000000000000000000000000000000000000000000',
  projection_event_seq INTEGER NOT NULL CHECK (projection_event_seq >= 1),
  projection_digest BLOB NOT NULL CHECK (length(projection_digest) = 32),
  updated_at TEXT NOT NULL,
  UNIQUE(layer, batch_seq),
  CHECK (layer IN (0, 1, 2)),
  CHECK (state IN (
    'open', 'sealed', 'compacting', 'compact_failed',
    'compacted', 'promoted', 'dropped'
  )),
  CHECK (est_tokens >= 0),
  CHECK (eviction_footprint_tokens >= 0),
  CHECK (membership_count >= 0),
  CHECK (length(membership_digest) = 32),
  CHECK (
    (summary_key_ref IS NULL AND summary_ciphertext IS NULL
      AND summary_projection IS NULL AND summary_redaction_version IS NULL)
    OR
    (summary_key_ref IS NOT NULL AND summary_ciphertext IS NOT NULL
      AND summary_projection IS NOT NULL AND summary_redaction_version IS NOT NULL)
  ),
  FOREIGN KEY(summary_key_ref) REFERENCES data_keys(key_ref),
  FOREIGN KEY(projection_event_seq) REFERENCES agent_events(seq)
    DEFERRABLE INITIALLY DEFERRED
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
  projection_event_seq INTEGER NOT NULL CHECK (projection_event_seq >= 1),
  projection_digest BLOB NOT NULL CHECK (length(projection_digest) = 32),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(kind, batch_seq),
  CHECK (status IN (
    'pending', 'running', 'completed', 'applied', 'discarded', 'failed'
  )),
  CHECK (
    (result_key_ref IS NULL AND result_ciphertext IS NULL
      AND result_projection IS NULL AND result_redaction_version IS NULL)
    OR
    (result_key_ref IS NOT NULL AND result_ciphertext IS NOT NULL
      AND result_projection IS NOT NULL AND result_redaction_version IS NOT NULL)
  ),
  FOREIGN KEY(result_key_ref) REFERENCES data_keys(key_ref),
  FOREIGN KEY(projection_event_seq) REFERENCES agent_events(seq)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_apply_cursors (
  kind TEXT NOT NULL PRIMARY KEY,
  next_batch_seq INTEGER NOT NULL,
  projection_event_seq INTEGER NOT NULL CHECK (projection_event_seq >= 1),
  projection_digest BLOB NOT NULL CHECK (length(projection_digest) = 32),
  FOREIGN KEY(projection_event_seq) REFERENCES agent_events(seq)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE memory_calibration (
  singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
  ratio_bits BLOB NOT NULL CHECK (length(ratio_bits) = 8),
  projection_event_seq INTEGER NOT NULL CHECK (projection_event_seq >= 1),
  projection_digest BLOB NOT NULL CHECK (length(projection_digest) = 32),
  FOREIGN KEY(projection_event_seq) REFERENCES agent_events(seq)
    DEFERRABLE INITIALLY DEFERRED
);

-- Provider-context rows, mutation replay intents, and Replace CAS heads are
-- independently committed because row-local AEAD cannot detect deletion.
-- The migration creates a mandatory
-- uninitialized marker rather than an optional head: Store may initialize it
-- only while the prelaunch provider-context state is empty.  A missing marker
-- or an uninitialized marker beside non-empty state fails closed.
CREATE TABLE provider_context_projection_head (
  singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  state TEXT NOT NULL CHECK (state IN ('uninitialized', 'active')),
  revision INTEGER NOT NULL CHECK (revision >= 0),
  record_count INTEGER NOT NULL CHECK (record_count >= 0),
  set_digest BLOB,
  key_ref TEXT,
  head_hmac BLOB,
  CHECK (
    (state = 'uninitialized'
      AND revision = 0
      AND record_count = 0
      AND set_digest IS NULL
      AND key_ref IS NULL
      AND head_hmac IS NULL)
    OR
    (state = 'active'
      AND set_digest IS NOT NULL
      AND length(set_digest) = 32
      AND key_ref IS NOT NULL
      AND head_hmac IS NOT NULL
      AND length(head_hmac) = 32)
  ),
  FOREIGN KEY(key_ref) REFERENCES data_keys(key_ref)
);

INSERT INTO provider_context_projection_head(
  singleton, schema_version, state, revision, record_count,
  set_digest, key_ref, head_hmac
) VALUES(1, 1, 'uninitialized', 0, 0, NULL, NULL, NULL);

CREATE TRIGGER reject_legacy_calibration_insert
BEFORE INSERT ON kv
WHEN NEW.key = 'calib.ratio'
BEGIN
  SELECT RAISE(ABORT, 'calib.ratio is reserved by memory_calibration');
END;

CREATE TRIGGER reject_legacy_calibration_update
BEFORE UPDATE ON kv
WHEN NEW.key = 'calib.ratio' OR OLD.key = 'calib.ratio'
BEGIN
  SELECT RAISE(ABORT, 'calib.ratio is reserved by memory_calibration');
END;

CREATE TRIGGER reject_legacy_calibration_delete
BEFORE DELETE ON kv
WHEN OLD.key = 'calib.ratio'
BEGIN
  SELECT RAISE(ABORT, 'calib.ratio is reserved by memory_calibration');
END;
