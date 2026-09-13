DROP TABLE IF EXISTS core_transfers;
ALTER TABLE core_personas
    DROP COLUMN IF EXISTS transfer_id,
    DROP COLUMN IF EXISTS authority;
