-- Reverse 0031 in dependency order. Apply this file as one transaction so the
-- prior schema is restored atomically.
DROP TABLE messaging_place_creation_receipts;

DROP TRIGGER places_increment_revision ON places;
DROP FUNCTION messaging_increment_place_revision();
ALTER TABLE places DROP COLUMN revision;

DROP INDEX participant_statuses_expiring;
DROP TRIGGER participant_statuses_increment_revision ON participant_statuses;
DROP FUNCTION messaging_increment_participant_status_revision();

-- The prior schema cannot represent a cleared declaration.
DELETE FROM participant_statuses WHERE status IS NULL;

ALTER TABLE participant_statuses
    ALTER COLUMN status SET NOT NULL;

ALTER TABLE participant_statuses
    DROP CONSTRAINT participant_statuses_base_needs_expiry,
    DROP COLUMN base_status,
    DROP COLUMN base_note,
    DROP COLUMN revision;
