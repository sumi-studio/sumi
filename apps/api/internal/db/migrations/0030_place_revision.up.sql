-- Place lifecycle frames are volatile. A client can receive an older edit
-- after a newer one, so each place exposes a monotonic projection revision.
ALTER TABLE places
    ADD COLUMN revision bigint NOT NULL DEFAULT 1
        CHECK (revision BETWEEN 1 AND 9007199254740991);

CREATE FUNCTION messaging_increment_place_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;
$$;

CREATE TRIGGER places_increment_revision
BEFORE UPDATE ON places
FOR EACH ROW EXECUTE FUNCTION messaging_increment_place_revision();
