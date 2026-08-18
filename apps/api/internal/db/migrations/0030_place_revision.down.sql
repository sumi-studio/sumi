DROP TRIGGER IF EXISTS places_increment_revision ON places;
DROP FUNCTION IF EXISTS messaging_increment_place_revision();
ALTER TABLE places DROP COLUMN IF EXISTS revision;
