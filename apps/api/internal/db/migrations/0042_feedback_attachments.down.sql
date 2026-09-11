DROP TABLE feedback_attachments;
-- Preserve existing larger reports while restoring the previous write bound.
ALTER TABLE feedback_threads DROP CONSTRAINT feedback_diagnostics_size;
ALTER TABLE feedback_threads ADD CONSTRAINT feedback_diagnostics_size CHECK (
    diagnostics IS NULL OR (jsonb_typeof(diagnostics) = 'object' AND octet_length(diagnostics::text) <= 16000)
) NOT VALID;
