DROP TABLE message_poll_votes;
DROP TABLE message_poll_options;
DROP TABLE message_polls;

-- 0018 の巻き戻し。thread place は 0008 の CHECK では表現できないので、
-- 先にスレッドの中身ごと落としてから列と制約を元の形へ戻す（pre-launch:
-- 守るべき本番データはない）。
DELETE FROM messages WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM read_markers WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM place_members WHERE place_id IN (SELECT place_id FROM places WHERE kind = 'thread');
DELETE FROM places WHERE kind = 'thread';

ALTER TABLE places
    DROP CONSTRAINT places_kind_known,
    DROP CONSTRAINT places_workspace_scoped,
    DROP CONSTRAINT places_named,
    DROP CONSTRAINT places_dm_keyed,
    DROP CONSTRAINT places_thread_parented,
    DROP CONSTRAINT places_thread_origin;

DROP INDEX places_one_thread_per_origin;
DROP INDEX places_by_parent;

ALTER TABLE places
    DROP COLUMN parent_message_id,
    DROP COLUMN parent_place_id;

ALTER TABLE places
    ADD CHECK (kind IN ('channel', 'dm', 'group_dm')),
    ADD CHECK ((kind = 'channel') = (workspace_id IS NOT NULL)),
    ADD CHECK ((kind = 'channel') = (name IS NOT NULL)),
    ADD CHECK ((kind = 'dm') = (dm_key IS NOT NULL));
