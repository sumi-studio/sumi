CREATE TABLE feedback_attachments (
    attachment_id uuidv7 PRIMARY KEY,
    author_key text NOT NULL,
    thread_id uuidv7 REFERENCES feedback_threads(thread_id),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    mime_type text NOT NULL CHECK (mime_type IN ('image/png','image/jpeg','image/webp','video/webm','video/mp4')),
    content bytea NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 20971520),
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    position integer
);
CREATE INDEX feedback_attachments_thread ON feedback_attachments(thread_id,position);
CREATE INDEX feedback_attachments_expiry ON feedback_attachments(expires_at) WHERE thread_id IS NULL;
ALTER TABLE feedback_threads DROP CONSTRAINT feedback_diagnostics_size;
ALTER TABLE feedback_threads ADD CONSTRAINT feedback_diagnostics_size CHECK (
    diagnostics IS NULL OR (jsonb_typeof(diagnostics) = 'object' AND octet_length(diagnostics::text) <= 40960)
);
