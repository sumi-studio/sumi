-- Persist wrapping-key identity beside the key bytes. Before this migration,
-- Store generated 32 random bytes encoded as unpadded base64url. Canonical
-- storage is now 64-character lowercase hex, so convert the representation
-- without changing the key bytes. The old schema did not record key identity;
-- the database cannot infer it safely. Historical wrapping_key_id therefore
-- remains NULL until an operator explicitly backfills the independently proven
-- ID. Runtime resolution fails closed while it is unresolved. An unrecognised
-- key value aborts this whole migration instead of silently stranding an agent.
ALTER TABLE agent_secrets
    ADD COLUMN wrapping_key_id text;

DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM agent_secrets
        WHERE NOT (
            wrapping_key ~ '^[0-9a-f]{64}$'
            OR CASE
                WHEN wrapping_key ~ '^[A-Za-z0-9_-]{43}$' THEN
                    octet_length(
                        decode(translate(wrapping_key, '-_', '+/') || '=', 'base64')
                    ) = 32
                    AND rtrim(
                        translate(
                            encode(
                                decode(translate(wrapping_key, '-_', '+/') || '=', 'base64'),
                                'base64'
                            ),
                            '+/',
                            '-_'
                        ),
                        '='
                    ) = wrapping_key
                ELSE false
            END
        )
    ) THEN
        RAISE EXCEPTION
            'agent_secrets contains a wrapping key that is neither canonical hex nor historical 32-byte base64url';
    END IF;
END
$migration$;

UPDATE agent_secrets
SET wrapping_key = CASE
        WHEN wrapping_key ~ '^[A-Za-z0-9_-]{43}$' THEN
            encode(
                decode(translate(wrapping_key, '-_', '+/') || '=', 'base64'),
                'hex'
            )
        ELSE wrapping_key
    END;

ALTER TABLE agent_secrets
    ADD CONSTRAINT agent_secrets_wrapping_key_canonical
    CHECK (wrapping_key ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT agent_secrets_wrapping_key_id_valid
    CHECK (
        wrapping_key_id IS NULL OR (
            wrapping_key_id = btrim(wrapping_key_id)
            AND length(wrapping_key_id) BETWEEN 1 AND 255
            AND wrapping_key_id !~ '[[:cntrl:]]'
        )
    );
