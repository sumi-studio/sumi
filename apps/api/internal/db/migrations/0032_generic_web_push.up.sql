-- Generic closed-tab Web Push. Notification decisions remain in
-- message_notification_intents; these tables only own the deployment key and
-- browser delivery endpoints.

CREATE TABLE push_vapid_keys (
    singleton   boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    public_key  text        NOT NULL CHECK (length(public_key) BETWEEN 1 AND 200),
    private_key text        NOT NULL CHECK (length(private_key) BETWEEN 1 AND 200),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE push_subscriptions (
    subscription_id    uuidv7      PRIMARY KEY,
    human_id           uuidv7      NOT NULL REFERENCES humans(human_id) ON DELETE CASCADE,
    browser_session_id text        NOT NULL CHECK (length(browser_session_id) = 43),
    session_expires_at timestamptz NOT NULL,
    endpoint           text        NOT NULL UNIQUE CHECK (length(endpoint) BETWEEN 1 AND 2000),
    p256dh             text        NOT NULL CHECK (length(p256dh) BETWEEN 1 AND 200),
    auth               text        NOT NULL CHECK (length(auth) BETWEEN 1 AND 100),
    owner_generation   bigint      NOT NULL DEFAULT 1 CHECK (owner_generation > 0),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX push_subscriptions_by_human
    ON push_subscriptions (human_id, updated_at DESC);

CREATE INDEX push_subscriptions_by_browser_session
    ON push_subscriptions (browser_session_id);
