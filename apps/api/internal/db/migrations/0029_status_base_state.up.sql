-- 0029_status_base_state: what a temporary status lapses back to.
--
-- 0014 gave participant_statuses an expires_at, but an expired row simply
-- stopped being reported: someone who had said「離席中」and then said「取り込み中、
-- 1時間だけ」came back an hour later as nothing at all, which readers show as
-- the default. That is the platform quietly editing a self-declaration.
--
-- The base columns hold what the participant had said before the temporary
-- state, so the lapse restores their own earlier words instead of erasing
-- them. They are meaningful only while expires_at is set; a status that holds
-- until replaced has nothing to fall back to and leaves them NULL/''.
ALTER TABLE participant_statuses
    ADD COLUMN base_status text
        CHECK (base_status IS NULL OR base_status IN ('available', 'busy', 'away')),
    ADD COLUMN base_note text NOT NULL DEFAULT ''
        CHECK (length(base_note) <= 200),
    -- A participant whose temporary declaration lapsed without a base keeps
    -- this row as a tombstone.  That lets the next declaration, and every
    -- reader of the clear, share one monotonic projection revision.
    ADD COLUMN revision bigint NOT NULL DEFAULT 1
        CHECK (revision BETWEEN 1 AND 9007199254740991);

-- A clear is a state too.  NULL is its durable representation; the old
-- CHECK already accepts NULL, so only the NOT NULL constraint must move.
ALTER TABLE participant_statuses
    ALTER COLUMN status DROP NOT NULL;

-- A base only means something for a status that will lapse.
ALTER TABLE participant_statuses
    ADD CONSTRAINT participant_statuses_base_needs_expiry
        CHECK (base_status IS NULL OR expires_at IS NOT NULL);

-- The expiry sweeper looks for rows that have lapsed. Partial on expires_at so
-- the many statuses that hold until replaced stay out of the index.
CREATE INDEX participant_statuses_expiring
    ON participant_statuses (expires_at)
    WHERE expires_at IS NOT NULL;

CREATE FUNCTION messaging_increment_participant_status_revision()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.revision := OLD.revision + 1;
    RETURN NEW;
END;
$$;

CREATE TRIGGER participant_statuses_increment_revision
BEFORE UPDATE ON participant_statuses
FOR EACH ROW EXECUTE FUNCTION messaging_increment_participant_status_revision();
