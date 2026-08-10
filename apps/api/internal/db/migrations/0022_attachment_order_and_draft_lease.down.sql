DROP INDEX message_attachments_unbound_by_expiry;
CREATE INDEX message_attachments_unbound_by_uploader
    ON message_attachments (uploader_kind, uploader_id)
    WHERE message_id IS NULL;

DROP INDEX message_attachments_by_message;
CREATE INDEX message_attachments_by_message
    ON message_attachments (message_id, created_at, attachment_id)
    WHERE message_id IS NOT NULL;

ALTER TABLE message_attachments
    DROP COLUMN draft_expires_at,
    DROP COLUMN position;
