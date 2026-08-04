-- 0013_message_attachments: file and image attachments for messaging
-- (docs/messaging-contracts-draft.md, issue #201).
--
-- An attachment is uploaded before the message that carries it exists, so the
-- row is minted unbound (message_id IS NULL) and bound at send time. The
-- uploader is recorded as the same (kind, id) participant pair used everywhere
-- else in the messaging schema, and it is the authorization basis twice over:
--   * only the uploader may bind an attachment to a message they send;
--   * an unbound attachment is readable by its uploader alone (a bound one is
--     readable by whoever can see the message's place).
-- Bytes live outside the database (local disk, sharded by attachment_id); this
-- table is the durable metadata and the visibility record.

CREATE TABLE message_attachments (
    attachment_id uuidv7      PRIMARY KEY,
    -- NULL until the uploader sends the message that carries it. A tombstoned
    -- message keeps its attachment rows as the record of what was sent, but
    -- the service stops delivering and serving them (AttachmentForViewer).
    message_id    uuidv7      REFERENCES messages(message_id),
    uploader_kind text        NOT NULL
        CHECK (uploader_kind IN ('human', 'personality_agent')),
    uploader_id   uuidv7      NOT NULL,
    filename      text        NOT NULL CHECK (length(filename) BETWEEN 1 AND 255),
    mime          text        NOT NULL CHECK (length(mime) BETWEEN 1 AND 255),
    -- 20 MiB, the same bound the upload endpoint enforces on the wire.
    size_bytes    bigint      NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 20971520),
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- Reading a message's attachments, and the "is it still unbound?" check the
-- binding update makes.
CREATE INDEX message_attachments_by_message
    ON message_attachments (message_id, created_at, attachment_id)
    WHERE message_id IS NOT NULL;

CREATE INDEX message_attachments_unbound_by_uploader
    ON message_attachments (uploader_kind, uploader_id)
    WHERE message_id IS NULL;
