-- 0023_message_attachments: Messaging file/image attachments with a durable
-- Workspace byte ledger, upload reservations, and a state-driven blob
-- deletion outbox.
--
-- Bytes live outside the database under the API-owned attachment root as
-- <root>/<id[0:2]>/<id[2:4]>/<id>.bin. These tables are the durable
-- identity, visibility, ordering, and quota record. Every relational key
-- carries workspace_id and place_id so a cross-workspace or cross-place bind
-- is impossible at the database level.

-- The one whole-store row is locked before every Workspace row. It bounds the
-- API-owned blob root even when a caller creates many Workspaces, and counts
-- objects as well as bytes so small files cannot exhaust inodes first.
CREATE TABLE message_attachment_store_usage (
    singleton     boolean      PRIMARY KEY DEFAULT true CHECK (singleton),
    used_bytes    bigint       NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    object_count  bigint       NOT NULL DEFAULT 0 CHECK (object_count >= 0),
    updated_at    timestamptz  NOT NULL DEFAULT now()
);

-- One row per Workspace. It covers reserved (in-flight) uploads, finalized
-- unbound drafts, and bound message attachments. Bytes and object counts leave
-- the ledger only in the same transaction that records reservation release or
-- confirms the blob is gone.
CREATE TABLE message_attachment_quotas (
    workspace_id uuidv7      PRIMARY KEY REFERENCES workspaces(workspace_id),
    used_bytes   bigint      NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    object_count bigint      NOT NULL DEFAULT 0 CHECK (object_count >= 0),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- A finalized upload. Minted only after its bytes are durable on disk. It is
-- unbound (message_id IS NULL) until its uploader sends a message that lists
-- it; from then on message history owns it.
--
-- blob_state is the durable deletion outbox: 'stored' means the bytes are
-- expected on disk, 'deleting' means a tombstone or draft expiry has queued
-- byte removal (the row is retained), and 'deleted' means removal was
-- confirmed and the ledger was released in the same transaction.
CREATE TABLE message_attachments (
    attachment_id   uuidv7      PRIMARY KEY,
    workspace_id    uuidv7      NOT NULL,
    place_id        uuidv7      NOT NULL,
    message_id      uuidv7,
    uploader_kind   text        NOT NULL
        CHECK (uploader_kind IN ('human', 'personality_agent')),
    uploader_id     uuidv7      NOT NULL,
    client_nonce    text        NOT NULL CHECK (octet_length(client_nonce) BETWEEN 1 AND 128),
    filename        text        NOT NULL CHECK (octet_length(filename) BETWEEN 1 AND 255),
    mime            text        NOT NULL CHECK (octet_length(mime) BETWEEN 1 AND 255),
    size_bytes      bigint      NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 20971520),
    sha256          bytea       NOT NULL CHECK (octet_length(sha256) = 32),
    -- The sender's chosen order within the message, assigned at bind time.
    position        integer     NOT NULL DEFAULT 0 CHECK (position >= 0 AND position < 10),
    blob_state      text        NOT NULL DEFAULT 'stored'
        CHECK (blob_state IN ('stored', 'deleting', 'deleted')),
    created_at      timestamptz NOT NULL DEFAULT now(),
    bound_at        timestamptz,
    blob_deleted_at timestamptz,
    CHECK ((message_id IS NULL) = (bound_at IS NULL)),
    CHECK ((blob_state = 'deleted') = (blob_deleted_at IS NOT NULL)),
    UNIQUE (workspace_id, place_id, attachment_id),
    CONSTRAINT message_attachments_place_uploader_nonce
        UNIQUE (workspace_id, place_id, uploader_kind, uploader_id, client_nonce),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id),
    FOREIGN KEY (workspace_id, place_id, message_id)
        REFERENCES messages (workspace_id, place_id, message_id)
);

-- Ordered projection of one message's attachments and the per-message
-- position uniqueness the bind protocol relies on.
CREATE UNIQUE INDEX message_attachments_message_position
    ON message_attachments (message_id, position)
    WHERE message_id IS NOT NULL;

-- Unbound drafts per uploader/place bound the outstanding draft budget and
-- feed the expiry reconciler.
CREATE INDEX message_attachments_unbound_drafts
    ON message_attachments (workspace_id, place_id, uploader_kind, uploader_id, created_at)
    WHERE message_id IS NULL AND blob_state = 'stored';

-- The deletion outbox worker reads exactly this set.
CREATE INDEX message_attachments_deleting
    ON message_attachments (created_at, attachment_id)
    WHERE blob_state = 'deleting';

-- Blob inventory the API expects on disk. Backup/restore tooling must use
-- this view instead of the raw table so retained-but-deleted metadata rows
-- are never mistaken for missing blobs.
CREATE VIEW message_attachment_blob_inventory AS
    SELECT attachment_id, size_bytes
    FROM message_attachments
    WHERE blob_state IN ('stored', 'deleting');

-- An upload reservation: quota is taken before any body byte is accepted.
-- installation_id/authority_epoch fence staging and finalization of this
-- exact upload only; they are never the visibility identity of the bound
-- attachment. A retry with the same per-file nonce resolves through this row
-- (still reserved) or through the finalized message_attachments row.
CREATE TABLE message_attachment_uploads (
    upload_id       uuidv7      PRIMARY KEY,
    workspace_id    uuidv7      NOT NULL,
    place_id        uuidv7      NOT NULL,
    uploader_kind   text        NOT NULL
        CHECK (uploader_kind IN ('human', 'personality_agent')),
    uploader_id     uuidv7      NOT NULL,
    client_nonce    text        NOT NULL CHECK (octet_length(client_nonce) BETWEEN 1 AND 128),
    installation_id text        NOT NULL CHECK (octet_length(installation_id) BETWEEN 1 AND 128),
    authority_epoch bigint      NOT NULL CHECK (authority_epoch >= 1),
    declared_bytes  bigint      NOT NULL CHECK (declared_bytes > 0 AND declared_bytes <= 20971520),
    state           text        NOT NULL DEFAULT 'reserved'
        CHECK (state IN ('reserved', 'finalized', 'released')),
    attachment_id   uuidv7      REFERENCES message_attachments(attachment_id),
    -- Exactly one durable body-staging claim may exist for a reservation.
    -- Finalization verifies this token, so a delayed claimant cannot publish
    -- after a retry has acquired a new claim.
    staging_token   uuidv7,
    staging_expires_at timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    settled_at      timestamptz,
    CHECK ((state = 'finalized') = (attachment_id IS NOT NULL)),
    CHECK ((state = 'reserved') = (settled_at IS NULL)),
    CHECK ((staging_token IS NULL) = (staging_expires_at IS NULL)),
    CHECK (expires_at > created_at),
    CONSTRAINT message_attachment_uploads_place_uploader_nonce
        UNIQUE (workspace_id, place_id, uploader_kind, uploader_id, client_nonce),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id)
);

CREATE INDEX message_attachment_uploads_reserved
    ON message_attachment_uploads (expires_at, upload_id)
    WHERE state = 'reserved';

CREATE INDEX message_attachment_uploads_settled
    ON message_attachment_uploads (settled_at, upload_id)
    WHERE state <> 'reserved';

-- Message send requests are compared by a canonical request digest on nonce
-- replay so a changed request under the same nonce is a conflict, never a
-- silent replay of the first message.
ALTER TABLE messages
    ADD COLUMN request_digest bytea NOT NULL
        CHECK (octet_length(request_digest) = 32);

-- Empty text is valid only when at least one attachment binds in the same
-- transaction. The trigger is deferred so the message insert can precede its
-- binds; the whole transaction rolls back if nothing binds.
CREATE OR REPLACE FUNCTION require_attachment_for_empty_message()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.deleted_at IS NULL AND NEW.content = '' AND NOT EXISTS (
        SELECT 1 FROM message_attachments a
        WHERE a.workspace_id = NEW.workspace_id
          AND a.place_id = NEW.place_id
          AND a.message_id = NEW.message_id
    ) THEN
        RAISE EXCEPTION 'a message with empty content must bind at least one attachment';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER message_empty_content_requires_attachment
    AFTER INSERT OR UPDATE OF content, deleted_at ON messages
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
    EXECUTE FUNCTION require_attachment_for_empty_message();
