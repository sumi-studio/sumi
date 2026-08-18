-- Reverse of 0029_status_base_state. Temporary statuses lose what they were
-- going to lapse back to; the rows themselves survive.
DROP INDEX IF EXISTS participant_statuses_expiring;

DROP TRIGGER IF EXISTS participant_statuses_increment_revision ON participant_statuses;
DROP FUNCTION IF EXISTS messaging_increment_participant_status_revision();

-- The old schema has no representation for a cleared declaration.
DELETE FROM participant_statuses WHERE status IS NULL;

ALTER TABLE participant_statuses
    ALTER COLUMN status SET NOT NULL;

ALTER TABLE participant_statuses
    DROP CONSTRAINT IF EXISTS participant_statuses_base_needs_expiry;

ALTER TABLE participant_statuses
    DROP COLUMN IF EXISTS base_status,
    DROP COLUMN IF EXISTS base_note,
    DROP COLUMN IF EXISTS revision;
