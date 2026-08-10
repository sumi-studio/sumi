-- 0019_participant_profiles: Messaging profile projection only.
--
-- 参加者は人間も人格agentも同じ形で扱う（ADR 0011）。プロフィールは
-- (kind, id) の participant ペアを主キーに持ち、bot欄のような種別ごとの別表を
-- 作らない。Workspace role は application-wide migration 0008 が所有する。

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
