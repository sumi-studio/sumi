-- The API's Direct Chat command logs, browser event logs and browser-session
-- revocations are append-oriented files under SUMI_COMMAND_LOG_DIR and
-- SUMI_BROWSER_EVENT_DIR. On a host whose disk does not survive replacement
-- (a Cloudflare Container, for example) the API mirrors every synced write of
-- those files here and restores them before it serves. PostgreSQL is then the
-- acknowledgement point: a write the API reported durable is in these tables.
--
-- Only one API process may write the mirror. Each process increments the
-- owner epoch when it starts, and every mirrored write re-checks that epoch in
-- its own transaction, so a replaced process can no longer change the files.

CREATE TABLE api_journal_mirror_owner (
    singleton   boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch       bigint      NOT NULL CHECK (epoch > 0),
    holder      text        NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT now()
);

-- One row per mirrored directory, keyed by a logical name (not a host path)
-- so the same state can be restored under a different mount point.
CREATE TABLE api_journal_mirror_dirs (
    dir          text        PRIMARY KEY CHECK (dir ~ '^[a-z][a-z0-9-]{0,62}$'),
    seeded_at    timestamptz NOT NULL DEFAULT now(),
    seeded_by    text        NOT NULL,
    seeded_files integer     NOT NULL CHECK (seeded_files >= 0),
    seeded_bytes bigint      NOT NULL CHECK (seeded_bytes >= 0)
);

CREATE TABLE api_journal_mirror_files (
    dir        text        NOT NULL REFERENCES api_journal_mirror_dirs (dir),
    name       text        NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._=-]{0,254}$'),
    size       bigint      NOT NULL CHECK (size >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dir, name)
);

-- File bytes in fixed 64 KiB chunks: an append rewrites only the chunks it
-- touches, not the whole log.
CREATE TABLE api_journal_mirror_chunks (
    dir   text   NOT NULL,
    name  text   NOT NULL,
    chunk bigint NOT NULL CHECK (chunk >= 0),
    data  bytea  NOT NULL CHECK (octet_length(data) BETWEEN 1 AND 65536),
    PRIMARY KEY (dir, name, chunk),
    FOREIGN KEY (dir, name) REFERENCES api_journal_mirror_files (dir, name) ON DELETE CASCADE
);
