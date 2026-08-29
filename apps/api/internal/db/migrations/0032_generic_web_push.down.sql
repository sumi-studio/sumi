DROP TABLE IF EXISTS push_subscriptions;
DROP TABLE IF EXISTS push_vapid_keys;

ALTER TABLE message_notification_intents
    DROP COLUMN IF EXISTS recipient_place_member_id,
    DROP COLUMN IF EXISTS recipient_workspace_member_id;
