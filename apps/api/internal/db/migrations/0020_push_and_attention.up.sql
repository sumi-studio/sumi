-- 0020_push_and_attention: 通知が「届く」ための配線。
--
-- 凍結契約 v1「Push 通知レイヤーとの対応」は、人間の Push 通知と agent の
-- AttentionCandidate を **同じ notification intent から分かれる別 adapter** と
-- 定めている。判定（all/mentions/mute/keyword、0015）はひとつのまま、その先の
-- 出口だけが二本に分かれる。だから両方の出口をひとつの migration に置く。
--
--   人間  : タブを閉じていても届く経路  → push_subscriptions / push_vapid_keys
--   agent : runtime が止まっていても残る → attention_candidates
--
-- attention_candidates は本 migration の時点ではまだ書き手を持たない
-- （同バッチの次レイヤーで attention.go が積み始める）。番号を分けても
-- 得るものが無いので同居させる。

-- 1. push_vapid_keys — この deployment ひとつぶんの VAPID 鍵対。
--    購読は鍵に紐づくので、鍵が入れ替わると既存購読はすべて無効になる。
--    したがって「無ければ作る、あれば絶対に作り直さない」を単一行制約で
--    データベース側の保証にする。private_key は push を送るためのもので、
--    人の秘密ではない。
CREATE TABLE push_vapid_keys (
    singleton   boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    public_key  text        NOT NULL,
    private_key text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- 2. push_subscriptions — 一人の人間の、一台ぶんの端末の受け口。
--    endpoint は push service が発行する URL で、これ自体が端末の識別子。
--    同じ endpoint が別の人に再発行されることはあるので、所有者ごとではなく
--    endpoint で一意にし、再購読は所有者を上書きする。
--
--    agent 側の対応物は attention_candidates であって「agent の push 購読」
--    ではない。agent はブラウザを持たない。同型性は「同じ判定から、それぞれの
--    身体に合った出口へ」であって、同じ配送方式を持つことではない。
CREATE TABLE push_subscriptions (
    subscription_id uuidv7      PRIMARY KEY,
    human_id        uuidv7      NOT NULL REFERENCES humans(human_id) ON DELETE CASCADE,
    endpoint        text        NOT NULL UNIQUE CHECK (length(endpoint) BETWEEN 1 AND 2000),
    -- 端末の公開鍵と認証秘密。web push の暗号化に使う（RFC 8291）。
    p256dh          text        NOT NULL CHECK (length(p256dh) BETWEEN 1 AND 200),
    auth            text        NOT NULL CHECK (length(auth) BETWEEN 1 AND 100),
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 送信ごとに「この人の端末は」を引く経路。
CREATE INDEX push_subscriptions_by_human ON push_subscriptions (human_id);

-- 3. attention_candidates — agent 向けの候補 queue（凍結契約 v1 §2 /
--    「AttentionCandidate の lifecycle」）。
--
--    正本は shared control plane 側にある、というのが契約の決定である
--    （runtime 停止中に届いたものを受け取れるのは shared 側だけ）。だから
--    これは agent-private DB ではなくここに置く。
--
--    candidate_seq は **agent ごとの単調増加**で、place の seq とは別軸。
--    consumed_at は ack の記録で、行は消さない：予算切れや runtime 再起動で
--    候補を捨てないという決定（ADR 0011 §9）に、消さないことで従う。
--
--    本バッチの実装は ADR 0010 の覚醒トリガ本設計より前の暫定配線である。
--    候補を積んで、本人が道具で取りに来られるところまでを作る。自動覚醒は
--    ここには無い。
CREATE TABLE attention_candidates (
    candidate_id  uuidv7      PRIMARY KEY,
    agent_id      uuidv7      NOT NULL REFERENCES agents(personality_agent_id) ON DELETE CASCADE,
    candidate_seq bigint      NOT NULL CHECK (candidate_seq > 0),
    place_id      uuidv7      NOT NULL REFERENCES places(place_id) ON DELETE CASCADE,
    message_seq   bigint      NOT NULL CHECK (message_seq > 0),
    -- 0015 の NotificationDecision.Reason と同じ語彙。「なぜ今これで呼ばれたか」
    -- は本人が見るので、推測ではなく判定そのものを運ぶ。
    reason        text        NOT NULL CHECK (reason IN ('dm', 'mention', 'keyword', 'all')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    consumed_at   timestamptz,
    -- 同じ message について同じ agent を二度呼ばない（at-least-once 配送の
    -- 重複は candidate_id で落とすが、発行そのものは冪等にしておく）。
    UNIQUE (agent_id, place_id, message_seq)
);

CREATE UNIQUE INDEX attention_candidates_agent_seq
    ON attention_candidates (agent_id, candidate_seq);

-- 未消化ぶんを古い順に取り出す経路。
CREATE INDEX attention_candidates_pending
    ON attention_candidates (agent_id, candidate_seq)
    WHERE consumed_at IS NULL;
