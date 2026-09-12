-- Existing session-bound subscriptions have no durable device cookie. Retire
-- those registrations; clients must renew registration after their next login.
-- Deployment VAPID keys and notification intent history remain intact.
DELETE FROM push_subscriptions;
CREATE TABLE push_devices (
    device_id text PRIMARY KEY CHECK (length(device_id) = 43),
    human_id uuidv7 NOT NULL REFERENCES humans(human_id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL
);
CREATE INDEX push_devices_by_human ON push_devices(human_id);
ALTER TABLE push_subscriptions
    DROP COLUMN browser_session_id,
    DROP COLUMN session_expires_at,
    ADD COLUMN device_id text NOT NULL REFERENCES push_devices(device_id) ON DELETE CASCADE;
CREATE INDEX push_subscriptions_by_device ON push_subscriptions(device_id);
