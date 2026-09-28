-- The API's Direct Chat command logs, browser event logs and browser-session
-- revocations are append-oriented files under SUMI_COMMAND_LOG_DIR and
-- SUMI_BROWSER_EVENT_DIR. On a host whose disk does not survive replacement
-- (a Cloudflare Container, for example) the API mirrors every synced write of
-- those files here and restores them before it serves. PostgreSQL is then the
-- acknowledgement point: a write the API reported durable is in these tables.
--
-- Only one API process may write the mirror. It holds a session advisory lock
-- (the lease) for its lifetime and increments the owner epoch when it starts;
-- every mirrored write re-checks that epoch in its own transaction, so a
-- replaced process can no longer change the files.

CREATE TABLE api_journal_mirror_owner (
    singleton   boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    epoch       bigint      NOT NULL CHECK (epoch > 0),
    holder      text        NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT now()
);

-- One row per mirrored directory, keyed by a logical name (not a host path)
-- so the same state can be restored under a different mount point. A row
-- exists only after an explicit initialization: adopting a host's existing
-- files, or declaring an empty mirror for a new installation. lineage
-- identifies this mirror; a host records it beside its local replica and
-- refuses to reconcile with a mirror of another lineage.
CREATE TABLE api_journal_mirror_dirs (
    dir               text        PRIMARY KEY CHECK (dir ~ '^[a-z][a-z0-9-]{0,62}$'),
    lineage           uuid        NOT NULL UNIQUE,
    initialized_how   text        NOT NULL CHECK (initialized_how IN ('adopted', 'empty')),
    initialized_at    timestamptz NOT NULL DEFAULT now(),
    initialized_by    text        NOT NULL,
    initialized_files integer     NOT NULL CHECK (initialized_files >= 0),
    initialized_bytes bigint      NOT NULL CHECK (initialized_bytes >= 0)
);

-- gen counts the committed changes of one file. A host records the gen of
-- its last acknowledged change beside the file; a mirror whose gen is lower
-- than that is older than what the host acknowledged (a restored backup, for
-- example) and is not reconciled over the host's files.
CREATE TABLE api_journal_mirror_files (
    dir        text        NOT NULL REFERENCES api_journal_mirror_dirs (dir),
    name       text        NOT NULL CHECK (name ~ '^[A-Za-z0-9][A-Za-z0-9._=-]{0,238}$'),
    size       bigint      NOT NULL CHECK (size >= 0),
    gen        bigint      NOT NULL DEFAULT 0 CHECK (gen >= 0),
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

-- The write path runs server-side so one synced write is one pipelined
-- round trip carrying only the written bytes.

-- Raises SQLSTATE SJ001 unless expected is the current owner epoch. The
-- shared row lock is held until commit, so an Acquire (which updates the row)
-- cannot interleave with a write that passed the check.
CREATE FUNCTION api_journal_mirror_check_owner(expected bigint) RETURNS void
    LANGUAGE plpgsql AS $$
DECLARE
    current_epoch bigint;
BEGIN
    SELECT epoch INTO current_epoch FROM api_journal_mirror_owner WHERE singleton FOR SHARE;
    IF current_epoch IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'journal mirror owner epoch is %, this writer holds %', current_epoch, expected
            USING ERRCODE = 'SJ001';
    END IF;
END
$$;

-- Writes data at byte offset p_offset of a mirrored file, like pwrite(2).
-- Writing past the end first extends the file with zero bytes.
CREATE FUNCTION api_journal_mirror_write(p_dir text, p_name text, p_offset bigint, p_data bytea) RETURNS void
    LANGUAGE plpgsql AS $$
DECLARE
    chunk_size constant bigint := 65536;
    file_size bigint;
    total bigint := octet_length(p_data);
    done bigint := 0;
    pos bigint;
    idx bigint;
    lo bigint;
    n bigint;
    part bytea;
BEGIN
    IF p_offset < 0 THEN
        RAISE EXCEPTION 'negative journal mirror offset %', p_offset;
    END IF;
    SELECT size INTO file_size FROM api_journal_mirror_files WHERE dir = p_dir AND name = p_name FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'journal mirror file %/% does not exist', p_dir, p_name;
    END IF;
    IF p_offset > file_size THEN
        PERFORM api_journal_mirror_truncate(p_dir, p_name, p_offset);
    END IF;
    WHILE done < total LOOP
        pos := p_offset + done;
        idx := pos / chunk_size;
        lo := pos - idx * chunk_size;
        n := least(chunk_size - lo, total - done);
        part := substring(p_data FROM (done + 1)::int FOR n::int);
        INSERT INTO api_journal_mirror_chunks AS c (dir, name, chunk, data)
        VALUES (p_dir, p_name, idx, decode(repeat('00', lo::int), 'hex') || part)
        ON CONFLICT (dir, name, chunk) DO UPDATE SET data =
            substring(c.data || decode(repeat('00', greatest(0, lo - octet_length(c.data))::int), 'hex') FROM 1 FOR lo::int)
            || part
            || substring(c.data FROM (lo + n + 1)::int);
        done := done + n;
    END LOOP;
    UPDATE api_journal_mirror_files SET size = greatest(size, p_offset + total), updated_at = now()
    WHERE dir = p_dir AND name = p_name;
END
$$;

-- Sets the size of a mirrored file, like ftruncate(2): shrinking drops the
-- bytes past p_size, growing appends zero bytes.
CREATE FUNCTION api_journal_mirror_truncate(p_dir text, p_name text, p_size bigint) RETURNS void
    LANGUAGE plpgsql AS $$
DECLARE
    chunk_size constant bigint := 65536;
    file_size bigint;
BEGIN
    IF p_size < 0 THEN
        RAISE EXCEPTION 'negative journal mirror size %', p_size;
    END IF;
    SELECT size INTO file_size FROM api_journal_mirror_files WHERE dir = p_dir AND name = p_name FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'journal mirror file %/% does not exist', p_dir, p_name;
    END IF;
    IF p_size > file_size THEN
        PERFORM api_journal_mirror_write(p_dir, p_name, file_size, decode(repeat('00', (p_size - file_size)::int), 'hex'));
        RETURN;
    END IF;
    IF p_size < file_size THEN
        DELETE FROM api_journal_mirror_chunks
        WHERE dir = p_dir AND name = p_name AND chunk >= (p_size + chunk_size - 1) / chunk_size;
        IF p_size % chunk_size <> 0 THEN
            UPDATE api_journal_mirror_chunks SET data = substring(data FROM 1 FOR (p_size % chunk_size)::int)
            WHERE dir = p_dir AND name = p_name AND chunk = p_size / chunk_size;
        END IF;
        UPDATE api_journal_mirror_files SET size = p_size, updated_at = now() WHERE dir = p_dir AND name = p_name;
    END IF;
END
$$;
