ALTER TABLE memory_jobs RENAME TO memory_jobs_before_discarded;

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
  FOREIGN KEY(result_key_ref) REFERENCES data_keys(key_ref)
);

INSERT INTO memory_jobs(
  id, kind, batch_seq, source_ids, source_versions, status, lease_until,
  attempts, result_key_ref, result_ciphertext, result_projection,
  result_redaction_version, created_at, updated_at
)
SELECT
  id, kind, batch_seq, source_ids, source_versions, status, lease_until,
  attempts, result_key_ref, result_ciphertext, result_projection,
  result_redaction_version, created_at, updated_at
FROM memory_jobs_before_discarded;

DROP TABLE memory_jobs_before_discarded;
