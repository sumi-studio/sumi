-- 0022_attachment_order_and_draft_lease: preserve the sender's attachment
-- order and make abandoned-upload reclamation respect live browser drafts.
--
-- 0013 is already a recorded migration on existing databases, so these
-- columns and indexes must be introduced forward rather than rewriting it.

ALTER TABLE message_attachments
    ADD COLUMN position integer,
    ADD COLUMN draft_expires_at timestamptz;

-- Existing messages used (created_at, attachment_id) as their display order.
-- Preserve exactly that observable order while assigning dense positions.
WITH ranked AS (
    SELECT attachment_id,
           (row_number() OVER (
               PARTITION BY message_id
               ORDER BY created_at, attachment_id
           ) - 1)::integer AS position
    FROM message_attachments
    WHERE message_id IS NOT NULL
)
UPDATE message_attachments a
SET position = ranked.position
FROM ranked
WHERE a.attachment_id = ranked.attachment_id;

UPDATE message_attachments
SET position = 0
WHERE message_id IS NULL;

-- Existing unbound uploads receive a full renewable lease from migration
-- time; an upgrade must not immediately reclaim a draft that was still open.
UPDATE message_attachments
SET draft_expires_at = now() + interval '7 days';

ALTER TABLE message_attachments
    ALTER COLUMN position SET DEFAULT 0,
    ALTER COLUMN position SET NOT NULL,
    ALTER COLUMN draft_expires_at SET DEFAULT (now() + interval '7 days'),
    ALTER COLUMN draft_expires_at SET NOT NULL,
    ADD CONSTRAINT message_attachments_position_range
        CHECK (position BETWEEN 0 AND 9);

DROP INDEX message_attachments_by_message;
CREATE UNIQUE INDEX message_attachments_by_message
    ON message_attachments (message_id, position)
    WHERE message_id IS NOT NULL;

DROP INDEX message_attachments_unbound_by_uploader;
CREATE INDEX message_attachments_unbound_by_expiry
    ON message_attachments (draft_expires_at, attachment_id)
    WHERE message_id IS NULL;
