-- 0029_push_and_attention: 0015 の notification intent に足を生やす二本の adapter。
-- 凍結契約 v1「Push 通知レイヤーとの対応」は、人間の Push と agent の
-- AttentionCandidate を **同じ notification intent から分かれる別 adapter** と
-- 定めている。判定はここでやり直さない：message_notification_intents が
-- message と同じ transaction で確定した正本で、この migration が足すのは
-- 「その答えを、閉じたタブの向こう／止まっていた runtime へどう届けるか」だけ。

-- 1. push_vapid_keys — この deployment の application server 鍵。鍵が変わると
--    全端末の購読が黙って死ぬので、「無ければ作る、あれば絶対に作り直さない」を
--    単一行制約として DB 側の保証にする（同時起動しても奪い合わない）。
CREATE TABLE push_vapid_keys (
    singleton   boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    public_key  text        NOT NULL CHECK (length(public_key) BETWEEN 1 AND 200),
    private_key text        NOT NULL CHECK (length(private_key) BETWEEN 1 AND 200),
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- 2. push_subscriptions — 一つのブラウザの push endpoint。Workspace ではなく
--    **人**に属する：ブラウザはその人の身体であって、Workspace の持ち物ではない。
--    同じ人が複数の Workspace にいても端末は一つで、何を送るかは intent 側が
--    Workspace ごとに既に決めている。
CREATE TABLE push_subscriptions (
    subscription_id uuidv7      PRIMARY KEY,
    human_id        uuidv7      NOT NULL REFERENCES humans(human_id) ON DELETE CASCADE,
    -- endpoint は push service が端末に発行する識別子であり、bearer secret でも
    -- ある。UNIQUE なのは、同じブラウザが再購読したときに行を増やさないため。
    endpoint        text        NOT NULL UNIQUE CHECK (length(endpoint) BETWEEN 1 AND 2000),
    p256dh          text        NOT NULL CHECK (length(p256dh) BETWEEN 1 AND 200),
    auth            text        NOT NULL CHECK (length(auth) BETWEEN 1 AND 100),
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX push_subscriptions_by_human ON push_subscriptions (human_id, created_at);

-- 3. attention_candidates — agent の per-agent inbox（凍結契約 v1
--    「AttentionCandidate の lifecycle」）。正本は shared control plane に置く、
--    という契約の決定にそのまま従う。
--
--    **行の中身は message_notification_intents からの projection である。**
--    候補を message commit の後でベストエフォートに積み直すと正本が二つになり、
--    commit と insert の間に候補を落とす窓ができる。ここでは intent を正本の
--    ままにして、本人が取りに来たときに未採番の intent を候補として numbering
--    する。落ちる窓が無く、番号は「本人が実際に受け取った順」になるので、
--    ack cursor の意味（candidate_seq 以下は配送済み）と一致する。
--
--    **暫定配線である。** 覚醒トリガの本設計（ADR 0010 / issue #173）では候補の
--    到着そのものが本人を起こす。ここには自動覚醒が無く、起きている本人が自分で
--    取りに来る。本設計が入るとき、この inbox は捨てるのではなく wake gate の
--    入力になる。
CREATE TABLE attention_candidates (
    candidate_id  uuidv7      PRIMARY KEY,
    workspace_id  uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    agent_id      uuidv7      NOT NULL,
    -- candidate_seq は agent ごとの単調増加（凍結契約 v1 §2）。place の seq とは
    -- 別軸なので、message_seq では ack の cursor になれない。
    candidate_seq bigint      NOT NULL CHECK (candidate_seq > 0),
    message_id    uuidv7      NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    place_id      uuidv7      NOT NULL,
    message_seq   bigint      NOT NULL CHECK (message_seq > 0),
    reason        text        NOT NULL CHECK (reason IN ('dm', 'mention', 'keyword', 'all')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    -- 行は消さない。予算切れや再起動を理由に候補を捨てないという決定
    -- （ADR 0011 §9）に、残すことで従う。
    consumed_at   timestamptz,
    UNIQUE (workspace_id, agent_id, candidate_seq),
    -- 同じ message で同じ agent を二度呼ばない。
    UNIQUE (workspace_id, agent_id, message_id),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id) ON DELETE CASCADE
);

CREATE INDEX attention_candidates_pending
    ON attention_candidates (workspace_id, agent_id, candidate_seq)
    WHERE consumed_at IS NULL;
