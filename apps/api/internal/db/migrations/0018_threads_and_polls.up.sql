-- 0018_threads_and_polls: スレッドと投票。
--
-- スレッドは新しい入れ物ではなく place の一種にする。seq・冪等送信・tombstone・
-- 既読マーカー・通知はすべて place 単位の既存の仕組みで動くので、thread を
-- places に追加するだけで会話の道具立てがそのまま効く（別テーブルにすると
-- 同じ規則を二度実装することになる）。
--
--   * parent_place_id   … 親チャンネル。スレッドは必ず親を持つ。
--   * parent_message_id … 起点メッセージ。null は「ゼロから作ったスレッド」。
--
-- 可視性は親チャンネルの workspace membership から引く（そのため thread も
-- workspace_id を持つ）。「参加者」は place_members に載る人 — 投稿した人と
-- 作成者で、未読と通知の対象はそちら。閲覧は親チャンネルのメンバー全員できる。

-- 1. places の kind を広げる。0008 の CHECK は無名で作られているため、名前を
--    決め打ちせず kind に言及する table-level/column-level CHECK を列挙して
--    落とし、以後は名前付きで貼り直す。
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
    ADD COLUMN parent_place_id   uuidv7 REFERENCES places(place_id),
    ADD COLUMN parent_message_id uuidv7 REFERENCES messages(message_id);

ALTER TABLE places
    ADD CONSTRAINT places_kind_known
        CHECK (kind IN ('channel', 'dm', 'group_dm', 'thread')),
    -- channel と thread は Workspace の中にあり、名前を持つ。dm/group_dm は
    -- どちらも持たない（表示名は参加者から導かれる）。
    ADD CONSTRAINT places_workspace_scoped
        CHECK ((kind IN ('channel', 'thread')) = (workspace_id IS NOT NULL)),
    ADD CONSTRAINT places_named
        CHECK ((kind IN ('channel', 'thread')) = (name IS NOT NULL)),
    ADD CONSTRAINT places_dm_keyed
        CHECK ((kind = 'dm') = (dm_key IS NOT NULL)),
    ADD CONSTRAINT places_thread_parented
        CHECK ((kind = 'thread') = (parent_place_id IS NOT NULL)),
    ADD CONSTRAINT places_thread_origin
        CHECK (parent_message_id IS NULL OR kind = 'thread');

CREATE INDEX places_by_parent ON places (parent_place_id)
    WHERE parent_place_id IS NOT NULL;

-- 1メッセージから生えるスレッドは1本。返信数チップが指す先が一意になる。
CREATE UNIQUE INDEX places_one_thread_per_origin ON places (parent_message_id)
    WHERE parent_message_id IS NOT NULL;
