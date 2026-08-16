DROP TRIGGER IF EXISTS message_empty_content_requires_attachment ON messages;
DROP FUNCTION IF EXISTS require_attachment_for_empty_message();
ALTER TABLE messages DROP COLUMN IF EXISTS request_digest;
DROP TABLE IF EXISTS message_attachment_uploads;
DROP VIEW IF EXISTS message_attachment_blob_inventory;
DROP TABLE IF EXISTS message_attachments;
DROP TABLE IF EXISTS message_attachment_quotas;
