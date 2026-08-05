ALTER TABLE message_attachments
    DROP COLUMN IF EXISTS alt,
    DROP COLUMN IF EXISTS spoiler;
