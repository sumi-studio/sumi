ALTER TABLE agent_secrets
    DROP CONSTRAINT IF EXISTS agent_secrets_wrapping_key_id_valid,
    DROP CONSTRAINT IF EXISTS agent_secrets_wrapping_key_canonical,
    DROP COLUMN IF EXISTS wrapping_key_id;
