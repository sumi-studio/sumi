ALTER TABLE feedback_threads ADD COLUMN diagnostics jsonb;
ALTER TABLE feedback_threads ADD CONSTRAINT feedback_diagnostics_size CHECK (
    diagnostics IS NULL OR (jsonb_typeof(diagnostics) = 'object' AND octet_length(diagnostics::text) <= 16000)
);
