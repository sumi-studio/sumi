-- Remove thread receipts before thread places: the receipt ledger owns an
-- exact foreign key to the created place.
DELETE FROM messaging_place_creation_receipts
WHERE operation = 'create_thread';

ALTER TABLE messaging_place_creation_receipts
    DROP CONSTRAINT messaging_place_creation_receipts_operation_check,
    ADD CONSTRAINT messaging_place_creation_receipts_operation_check
        CHECK (operation IN ('create_channel', 'duplicate_channel', 'create_group_dm'));

DROP TABLE read_markers;

CREATE TABLE read_markers (
    place_id        uuidv7      NOT NULL,
    place_member_id uuidv7      NOT NULL,
    last_read_seq   bigint      NOT NULL
        CHECK (last_read_seq >= 0 AND last_read_seq <= 9007199254740991),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, place_member_id),
    FOREIGN KEY (place_id, place_member_id)
        REFERENCES place_members (place_id, place_member_id)
);

DELETE FROM notification_setting_places
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM reply_later_markers
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM message_attachment_uploads
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM message_attachments
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM messages
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM place_members
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM places WHERE kind = 'thread';

DROP INDEX places_one_thread_per_origin;
DROP INDEX places_by_parent;

ALTER TABLE places
    DROP CONSTRAINT places_kind_known,
    DROP CONSTRAINT places_named,
    DROP CONSTRAINT places_dm_keyed,
    DROP CONSTRAINT places_thread_parented,
    DROP CONSTRAINT places_thread_origin,
    DROP CONSTRAINT places_thread_name_length,
    DROP CONSTRAINT places_voice_is_channel_only,
    DROP CONSTRAINT places_parent_message_fk,
    DROP CONSTRAINT places_parent_place_fk,
    DROP COLUMN parent_message_id,
    DROP COLUMN parent_place_id,
    ADD CHECK (kind IN ('channel', 'dm', 'group_dm')),
    ADD CHECK ((kind = 'channel') = (name IS NOT NULL)),
    ADD CHECK ((kind = 'dm') = (dm_key IS NOT NULL)),
    ADD CONSTRAINT places_voice_is_channel_only
        CHECK (NOT voice OR kind = 'channel');
