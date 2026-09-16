DROP INDEX IF EXISTS auth_flows_browser_epoch;
ALTER TABLE auth_flows
    DROP COLUMN browser_epoch_hash,
    DROP COLUMN closed_at;
