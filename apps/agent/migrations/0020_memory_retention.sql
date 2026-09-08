-- KEEP_UNCHANGED is a successful terminal compaction outcome. Original L0
-- memberships already have durable authenticated storage and remain there
-- after the batch leaves the active prompt; no archive table is needed.
ALTER TABLE memory_jobs RENAME TO memory_jobs_before_unchanged;

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
    'pending', 'running', 'completed', 'applied', 'unchanged', 'discarded', 'failed'
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

INSERT INTO memory_jobs(
  id, kind, batch_seq, source_ids, source_versions, status, lease_until,
  attempts, result_key_ref, result_ciphertext, result_projection,
  result_redaction_version, projection_event_seq, projection_digest,
  created_at, updated_at
)
SELECT
  id, kind, batch_seq, source_ids, source_versions, status, lease_until,
  attempts, result_key_ref, result_ciphertext, result_projection,
  result_redaction_version, projection_event_seq, projection_digest,
  created_at, updated_at
FROM memory_jobs_before_unchanged;

DROP TABLE memory_jobs_before_unchanged;
