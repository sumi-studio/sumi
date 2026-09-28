-- Messaging attachment bytes for hosts without a durable local disk
-- (SUMI_MESSAGING_ATTACHMENT_STORE=postgres). Attachment metadata, quotas and
-- lifecycle stay in messaging_attachments; this is only the blob store behind
-- messaging.AttachmentBlobs. A blob is 'staging' while its upload streams in
-- and 'published' once the upload's metadata commits; both may exist for one
-- attachment, as the staging and final files do on disk.
CREATE TABLE messaging_attachment_blobs (
    blob_id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attachment_id text        NOT NULL,
    kind          text        NOT NULL CHECK (kind IN ('staging', 'published')),
    size          bigint      NOT NULL DEFAULT 0 CHECK (size >= 0),
    complete      boolean     NOT NULL DEFAULT false,
    changed_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (attachment_id, kind)
);

CREATE TABLE messaging_attachment_blob_chunks (
    blob_id bigint  NOT NULL REFERENCES messaging_attachment_blobs (blob_id) ON DELETE CASCADE,
    chunk   integer NOT NULL CHECK (chunk >= 0),
    data    bytea   NOT NULL CHECK (octet_length(data) BETWEEN 1 AND 262144),
    PRIMARY KEY (blob_id, chunk)
);
