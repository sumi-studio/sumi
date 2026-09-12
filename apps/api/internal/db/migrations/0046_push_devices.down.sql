-- Durable device registrations cannot be converted back to short HTTP sessions.
DELETE FROM push_subscriptions;
ALTER TABLE push_subscriptions
    DROP COLUMN device_id,
    ADD COLUMN browser_session_id text NOT NULL CHECK (length(browser_session_id) = 43),
    ADD COLUMN session_expires_at timestamptz NOT NULL;
CREATE INDEX push_subscriptions_by_browser_session ON push_subscriptions(browser_session_id);
DROP TABLE push_devices;
