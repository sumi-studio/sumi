-- 0029_message_threads: channel-scoped side conversations are ordinary places.
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
    ADD CONSTRAINT places_voice_is_channel_only
        CHECK (NOT voice OR kind = 'channel');

CREATE INDEX places_by_parent ON places (workspace_id, parent_place_id, created_at, place_id)
    WHERE parent_place_id IS NOT NULL;
CREATE UNIQUE INDEX places_one_thread_per_origin ON places (parent_message_id)
    WHERE parent_message_id IS NOT NULL;
