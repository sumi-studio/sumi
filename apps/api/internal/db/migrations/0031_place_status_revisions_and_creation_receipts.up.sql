-- 0031_place_status_revisions_and_creation_receipts: make temporary status
-- restoration and place lifecycle projections monotonic, and make retried
-- place creation return its original committed result. The migration runner
-- applies this whole file in one transaction.

-- 0014 gave participant_statuses an expires_at, but an expired row simply
-- stopped being reported. Preserve the participant's previous declaration so
-- a temporary status lapses back to their own words rather than to a default.
ALTER TABLE participant_statuses
    ADD COLUMN base_status text
        CHECK (base_status IS NULL OR base_status IN ('available', 'busy', 'away')),
    ADD COLUMN base_note text NOT NULL DEFAULT ''
        CHECK (length(base_note) <= 200),
    -- A clear is also a monotonic projection state. A lapsed temporary status
    -- without a base remains as a NULL-status tombstone at its new revision.
    ADD COLUMN revision bigint NOT NULL DEFAULT 1
        CHECK (revision BETWEEN 1 AND 9007199254740991);

ALTER TABLE participant_statuses
    ALTER COLUMN status DROP NOT NULL;

ALTER TABLE participant_statuses
    ADD CONSTRAINT participant_statuses_base_needs_expiry
        CHECK (base_status IS NULL OR expires_at IS NOT NULL);

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

-- Place lifecycle frames are volatile, so each channel/DM projection carries
-- a monotonic revision that lets clients reject an older late arrival.
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

-- A response can be lost after a channel, copy, or group DM commits. The
-- authenticated actor's exact Workspace membership tenure and nonce record
-- the creation result so a bounded retry returns that place without exposing
-- it to the same participant after they leave and rejoin under a new tenure.
CREATE TABLE messaging_place_creation_receipts (
    workspace_id        uuidv7 NOT NULL,
    workspace_member_id uuidv7 NOT NULL,
    member_kind         text   NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id           uuidv7 NOT NULL,
    operation           text   NOT NULL
        CHECK (operation IN ('create_channel', 'duplicate_channel', 'create_group_dm')),
    client_nonce        text   NOT NULL CHECK (length(client_nonce) BETWEEN 1 AND 128),
    request_digest      bytea  NOT NULL CHECK (octet_length(request_digest) = 32),
    place_id            uuidv7 NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, workspace_member_id, operation, client_nonce),
    CONSTRAINT messaging_place_creation_receipts_workspace_member_identity
        FOREIGN KEY (workspace_id, workspace_member_id, member_kind, member_id)
        REFERENCES workspace_members
            (workspace_id, workspace_member_id, member_kind, member_id),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id)
);
