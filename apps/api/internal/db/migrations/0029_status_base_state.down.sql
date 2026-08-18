-- Reverse of 0029_status_base_state. Temporary statuses lose what they were
-- going to lapse back to; the rows themselves survive.
DROP INDEX IF EXISTS participant_statuses_expiring;

ALTER TABLE participant_statuses
    DROP CONSTRAINT IF EXISTS participant_statuses_base_needs_expiry;

ALTER TABLE participant_statuses
    DROP COLUMN IF EXISTS base_status,
    DROP COLUMN IF EXISTS base_note;
