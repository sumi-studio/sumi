DROP TABLE IF EXISTS core_transfers;
DROP TABLE IF EXISTS core_placement;
ALTER TABLE core_personas
    DROP COLUMN IF EXISTS transfer_id,
    DROP COLUMN IF EXISTS authority;
