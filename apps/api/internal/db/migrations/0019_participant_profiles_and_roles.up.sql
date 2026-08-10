-- 0019_participant_profiles_and_roles: 個人設定とワークスペース管理の最小モデル。
--
-- 参加者は人間も人格agentも同じ形で扱う（ADR 0011）。プロフィールもロールも
-- (kind, id) の participant ペアを主キーに持ち、bot欄のような種別ごとの別表を
-- 作らない。

-- 1. participant_profiles — 本人が名乗るもの。表示名は戸籍（humans / agents）が
--    正本のままで、ここには「システムが付ける分類」ではなく本人が足した説明と
--    画像だけが載る。画像は既存の添付基盤（message_attachments）を再利用し、
--    メッセージに紐付いていない添付を参照する。
CREATE TABLE participant_profiles (
    member_kind          text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id            uuidv7      NOT NULL,
    -- 職務の説明（例: 秘書、開発）。bot badge ではない。
    tagline              text        NOT NULL DEFAULT ''
        CHECK (char_length(tagline) <= 100),
    avatar_attachment_id uuidv7      REFERENCES message_attachments(attachment_id),
    banner_attachment_id uuidv7      REFERENCES message_attachments(attachment_id),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (member_kind, member_id)
);

-- プロフィール画像として参照されている添付を、可視性判定から素早く引く。
CREATE INDEX participant_profiles_by_avatar
    ON participant_profiles (avatar_attachment_id)
    WHERE avatar_attachment_id IS NOT NULL;
CREATE INDEX participant_profiles_by_banner
    ON participant_profiles (banner_attachment_id)
    WHERE banner_attachment_id IS NOT NULL;

-- 2. workspace_roles — ワークスペースの権限の束。Discord 的な「ロール」を最小の
--    4 権限で表す。permissions は jsonb の真偽値の集合で、未知のキーは
--    fail-closed に無視される（増やすときに migration が要らない形）。
--
--    最小の権限キー:
--      manage_channels … チャンネルの作成・編集・複製・削除
--      manage_roles    … ロールの作成・編集・削除
--      manage_members  … メンバーへのロール付与・変更
--      mention_all     … @everyone 相当（将来使うときのための場所取り）
CREATE TABLE workspace_roles (
    role_id      uuidv7      PRIMARY KEY,
    workspace_id uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    name         text        NOT NULL CHECK (char_length(name) BETWEEN 1 AND 60),
    -- NULL は「色を付けない」。表示は #rrggbb の小文字だけを受け付ける。
    color        text        CHECK (color IS NULL OR color ~ '^#[0-9a-f]{6}$'),
    position     integer     NOT NULL DEFAULT 0,
    permissions  jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, name)
);

CREATE INDEX workspace_roles_by_workspace ON workspace_roles (workspace_id);

-- 3. participant_roles — 誰がどのロールを持つか。人間も人格agentも同じ形で付く。
CREATE TABLE participant_roles (
    role_id     uuidv7 NOT NULL REFERENCES workspace_roles(role_id) ON DELETE CASCADE,
    member_kind text   NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7 NOT NULL,
    granted_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, member_kind, member_id)
);

CREATE INDEX participant_roles_by_participant
    ON participant_roles (member_kind, member_id);

-- 共有 MVP ワークスペースの初期ロール。ID は default workspace と同じく
-- 製品上の固定 identity にして、レプリカ間の競合を避ける。
INSERT INTO workspace_roles (role_id, workspace_id, name, position, permissions)
VALUES
    ('01900000-0000-7000-8000-000000000003',
     '01900000-0000-7000-8000-000000000001',
     'Admin', 100,
     '{"manage_channels": true, "manage_roles": true,
       "manage_members": true, "mention_all": true}'::jsonb),
    ('01900000-0000-7000-8000-000000000004',
     '01900000-0000-7000-8000-000000000001',
     'Member', 0, '{}'::jsonb);

-- pre-launch の前提: いま戸籍にいる human は全員が創業メンバーなので Admin。
-- これ以降に参加する human は権限なし（= Member 相当）で始まり、Admin が
-- 明示的に付与する。agent participant にはロールを付けない。
INSERT INTO participant_roles (role_id, member_kind, member_id)
SELECT '01900000-0000-7000-8000-000000000003', 'human', human_id FROM humans
ON CONFLICT DO NOTHING;
