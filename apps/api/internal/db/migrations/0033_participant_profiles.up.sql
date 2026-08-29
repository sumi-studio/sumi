-- 0033_participant_profiles: the canonical Participant profile.
--
-- 表示名は戸籍（humans / agents）が正本であり続ける。この表が持つのは、参加者が
-- 自分について名乗った一行 — tagline だけ。Workspace membership ではなく
-- Participant に属させるのは、人格・本人性が Workspace をまたいで続くから。
-- Workspace 固有の肩書きが要るようになったら、global tagline を上書きせず
-- membership 側の descriptor として別に持たせる。
--
-- participant_statuses と同じ (kind, id) 形。Human と PersonalityAgent は同じ
-- 表の同じ列に載る（AX: 名乗りは片方だけの能力ではない）。
CREATE TABLE participant_profiles (
    member_kind text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7      NOT NULL,
    -- 職務の説明（例: 秘書、開発）を一行で。伝記ではないので短く縛る。
    tagline     text        NOT NULL DEFAULT '' CHECK (char_length(tagline) <= 100),
    PRIMARY KEY (member_kind, member_id)
);
