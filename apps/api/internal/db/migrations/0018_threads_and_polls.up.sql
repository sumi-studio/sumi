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
    ADD COLUMN parent_place_id   uuidv7,
    ADD COLUMN parent_message_id uuidv7,
    ADD CONSTRAINT places_parent_same_workspace
        FOREIGN KEY (workspace_id, parent_place_id)
        REFERENCES places (workspace_id, place_id),
    ADD CONSTRAINT places_origin_in_parent
        FOREIGN KEY (parent_place_id, parent_message_id)
        REFERENCES messages (place_id, message_id);

ALTER TABLE places
    ADD CONSTRAINT places_kind_known
        CHECK (kind IN ('channel', 'dm', 'group_dm', 'thread')),
    -- Every Messaging place belongs to a Workspace. channel/thread have a
    -- name; dm/group_dm derive their display name from participants.
    ADD CONSTRAINT places_workspace_scoped
        CHECK (workspace_id IS NOT NULL),
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

-- 2. 投票。メッセージが投票を運ぶ（別の入れ物ではない）ので、投票は
--    message_id を主キーに持つ1対1の付属物。tombstone化した発言と一緒に
--    消える（ON DELETE CASCADE ＋ store側のtombstone処理）。
CREATE TABLE message_polls (
    message_id  uuidv7      PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
    question    text        NOT NULL CHECK (length(question) BETWEEN 1 AND 500),
    -- 複数選択可か。falseなら「同一pollに1票」をサーバーが強制する。
    allow_multi boolean     NOT NULL DEFAULT false,
    -- 締切。NULL は締切なし。過ぎたら結果だけが見える。
    closes_at   timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE message_poll_options (
    option_id  uuidv7 PRIMARY KEY,
    message_id uuidv7 NOT NULL REFERENCES message_polls(message_id) ON DELETE CASCADE,
    text       text   NOT NULL CHECK (length(text) BETWEEN 1 AND 200),
    -- 表示順。作成時の並びをそのまま保つ。
    ord        int    NOT NULL CHECK (ord >= 0),
    UNIQUE (message_id, ord)
);

-- 投票者は human と personality_agent の同じ形。誰が入れたかは reactions と
-- 同じく見える（匿名投票はv0では作らない）。message_id を冗長に持つのは
-- 「同一pollに1票」をサーバーが1本のDELETEで強制できるようにするため。
CREATE TABLE message_poll_votes (
    option_id  uuidv7      NOT NULL REFERENCES message_poll_options(option_id) ON DELETE CASCADE,
    message_id uuidv7      NOT NULL REFERENCES message_polls(message_id) ON DELETE CASCADE,
    voter_kind text        NOT NULL
        CHECK (voter_kind IN ('human', 'personality_agent')),
    voter_id   uuidv7      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (option_id, voter_kind, voter_id)
);

CREATE INDEX message_poll_votes_by_poll ON message_poll_votes (message_id);
CREATE INDEX message_poll_options_by_poll ON message_poll_options (message_id, ord);
