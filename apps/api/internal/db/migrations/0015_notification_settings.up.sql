-- 0015_notification_settings: 受信側が自分の通知条件を持つ
-- (docs/messaging-contracts-draft.md「ReadMarker と NotificationSetting —
-- HumanもAgentも同じ形」)。owner が本人、human/agent は同型の resource であり、
-- 変更できるのは本人だけ。人間はUI、agentはtool — 同じ契約の別transport。
--
-- 通知の「発火するか」は送信時にサーバー側で評価する。受信者ごとの判定を
-- クライアントに委ねると、mute した place の本文が結局その端末まで届いてから
-- 捨てられることになり、受信側制御と呼べない。

-- 1. notification_settings — 一人ぶんの既定と keyword。行が無いことは
--    「まだ何も言っていない」であって未設定エラーではないので、読み出し側は
--    defaults_level の DEFAULT と同じ既定値へ落とす。
CREATE TABLE notification_settings (
    workspace_id   uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    member_kind    text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id      uuidv7      NOT NULL,
    defaults_level text        NOT NULL DEFAULT 'all'
        CHECK (defaults_level IN ('all', 'mentions', 'mute')),
    -- 自分の名前以外で呼ばれたい言葉。空配列は「keyword を使わない」。
    keywords       text[]      NOT NULL DEFAULT '{}'
        CHECK (cardinality(keywords) <= 32),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, member_kind, member_id)
);

-- 2. notification_setting_places — place 単位の上書き。既定と同じ値でも
--    「その place について明示的にそう決めた」という別の事実なので、行を
--    残すか消すかは本人の操作がそのまま反映される。
CREATE TABLE notification_setting_places (
    workspace_id uuidv7      NOT NULL,
    member_kind text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7      NOT NULL,
    place_id    uuidv7      NOT NULL,
    level       text        NOT NULL CHECK (level IN ('all', 'mentions', 'mute')),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, member_kind, member_id, place_id),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id, member_kind, member_id)
        REFERENCES notification_settings
            (workspace_id, member_kind, member_id) ON DELETE CASCADE
);

-- 送信ごとに「この place を mute している人は誰か」を引く経路。
CREATE INDEX notification_setting_places_by_place
    ON notification_setting_places (workspace_id, place_id);

-- 3. message_notification_intents — message と同じ transaction で発行する
--    typed intent。live WebSocket / Push / AttentionCandidate はこの正本から
--    best-effort に配送できるが、配送失敗を message commit の失敗にはしない。
--    recipient と reason は admission 時の判定を固定し、message commit 後の
--    membership・setting 変更で過去の intent を書き換えない。
CREATE TABLE message_notification_intents (
    message_id      uuidv7      NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    recipient_kind  text        NOT NULL
        CHECK (recipient_kind IN ('human', 'personality_agent')),
    recipient_id    uuidv7      NOT NULL,
    reason          text        NOT NULL
        CHECK (reason IN ('dm', 'mention', 'keyword', 'all')),
    issued_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, recipient_kind, recipient_id)
);

CREATE INDEX message_notification_intents_by_recipient
    ON message_notification_intents (recipient_kind, recipient_id, issued_at);
