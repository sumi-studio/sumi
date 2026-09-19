DROP INDEX IF EXISTS persona_file_tokens_active;
DROP TABLE IF EXISTS persona_file_tokens;
ALTER TABLE return_sessions DROP COLUMN IF EXISTS file_epoch;
DROP SEQUENCE IF EXISTS return_file_epoch_seq;
ALTER TABLE return_sessions DROP COLUMN IF EXISTS file_mode;
ALTER TABLE return_sessions DROP CONSTRAINT IF EXISTS return_sessions_capture_binding;
ALTER TABLE return_sessions DROP COLUMN IF EXISTS capture_manifest_sha;
ALTER TABLE return_sessions DROP COLUMN IF EXISTS capture_scope_id;
ALTER TABLE return_sessions DROP COLUMN IF EXISTS capture_id;
