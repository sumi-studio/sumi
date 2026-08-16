-- Humans created before Direct Chat became a default app need the same enabled
-- lifecycle binding as newly provisioned accounts. Keep an existing disabled
-- or uninstalled choice intact: only absent bindings are inserted.
WITH missing AS (
    SELECT h.human_id,
           md5('sumi:direct-chat-backfill:v1:' || h.human_id) AS digest
    FROM humans h
    WHERE NOT EXISTS (
        SELECT 1
        FROM app_installations ai
        WHERE ai.owner_kind = 'human'
          AND ai.owner_id = h.human_id
          AND ai.app_id = 'direct-chat'
    )
)
INSERT INTO app_installations
    (installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
     installed_at, updated_at)
SELECT
    -- Retain the Human's UUIDv7 timestamp prefix and derive the remaining
    -- opaque installation bits deterministically for a retry-safe migration.
    substr(human_id, 1, 14) || '7' || substr(digest, 1, 3) || '-8' ||
        substr(digest, 4, 3) || '-' || substr(digest, 7, 12),
    'human', human_id, 'direct-chat', true, 1, now(), now()
FROM missing
ON CONFLICT ON CONSTRAINT app_installations_owner_kind_owner_id_app_id_key
DO NOTHING;
