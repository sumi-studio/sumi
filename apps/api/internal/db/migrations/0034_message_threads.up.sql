-- 0034_message_threads: channel-scoped side conversations are ordinary
-- places. A visible nonparticipant may keep a read cursor without becoming a
-- thread member, and thread creation reuses the existing exact-tenure place
-- creation receipt ledger.
DO $$
DECLARE constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'places'::regclass AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%kind%'
    LOOP
        EXECUTE format('ALTER TABLE places DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END $$;

ALTER TABLE places
    ADD COLUMN parent_place_id uuidv7,
    ADD COLUMN parent_message_id uuidv7,
    ADD CONSTRAINT places_parent_place_fk
        FOREIGN KEY (workspace_id, parent_place_id)
        REFERENCES places (workspace_id, place_id),
    ADD CONSTRAINT places_parent_message_fk
        FOREIGN KEY (workspace_id, parent_place_id, parent_message_id)
        REFERENCES messages (workspace_id, place_id, message_id),
    ADD CONSTRAINT places_kind_known
        CHECK (kind IN ('channel', 'dm', 'group_dm', 'thread')),
    ADD CONSTRAINT places_named
        CHECK ((kind IN ('channel', 'thread')) = (name IS NOT NULL)),
    ADD CONSTRAINT places_dm_keyed
        CHECK ((kind = 'dm') = (dm_key IS NOT NULL)),
    ADD CONSTRAINT places_thread_parented
        CHECK ((kind = 'thread') = (parent_place_id IS NOT NULL)),
    ADD CONSTRAINT places_thread_origin
        CHECK (parent_message_id IS NULL OR kind = 'thread'),
    ADD CONSTRAINT places_thread_name_length
        CHECK (kind <> 'thread' OR char_length(name) <= 100),
    ADD CONSTRAINT places_voice_is_channel_only
        CHECK (NOT voice OR kind = 'channel');

CREATE INDEX places_by_parent ON places (workspace_id, parent_place_id, created_at, place_id)
    WHERE parent_place_id IS NOT NULL;
CREATE UNIQUE INDEX places_one_thread_per_origin ON places (parent_message_id)
    WHERE parent_message_id IS NOT NULL;

-- The previous key was a place membership tenure. A nonparticipant thread
-- viewer has no such row; their exact Workspace tenure is the durable owner.
DROP TABLE read_markers;

CREATE TABLE read_markers (
    place_id            uuidv7      NOT NULL REFERENCES places (place_id),
    workspace_member_id uuidv7      NOT NULL REFERENCES workspace_members (workspace_member_id),
    last_read_seq       bigint      NOT NULL
        CHECK (last_read_seq >= 0 AND last_read_seq <= 9007199254740991),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, workspace_member_id)
);

ALTER TABLE messaging_place_creation_receipts
    DROP CONSTRAINT messaging_place_creation_receipts_operation_check,
    ADD CONSTRAINT messaging_place_creation_receipts_operation_check
        CHECK (operation IN (
            'create_channel',
            'duplicate_channel',
            'create_group_dm',
            'create_thread'
        ));
