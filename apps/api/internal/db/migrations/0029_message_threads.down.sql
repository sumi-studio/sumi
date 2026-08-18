DELETE FROM notification_setting_places
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM reply_later_markers
WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM read_markers
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
