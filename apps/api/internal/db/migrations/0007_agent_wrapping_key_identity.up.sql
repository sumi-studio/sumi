-- Persist wrapping-key identity beside the key bytes. Existing rows remain
-- explicitly unresolved until an operator assigns the proven historical ID;
-- runtime resolution never falls back to a process-global identifier.
ALTER TABLE agent_secrets
    ADD COLUMN wrapping_key_id text;

ALTER TABLE agent_secrets
    ADD CONSTRAINT agent_secrets_wrapping_key_id_valid
    CHECK (
        wrapping_key_id IS NULL OR (
            wrapping_key_id = btrim(wrapping_key_id)
            AND length(wrapping_key_id) BETWEEN 1 AND 255
            AND wrapping_key_id !~ '[[:cntrl:]]'
        )
    );
