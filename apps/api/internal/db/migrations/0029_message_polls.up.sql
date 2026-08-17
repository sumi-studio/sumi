-- 0029_message_polls: a poll is the one-to-one attachment of a message.
CREATE TABLE message_polls (
    workspace_id uuidv7      NOT NULL,
    message_id   uuidv7      NOT NULL,
    question     text        NOT NULL CHECK (length(question) BETWEEN 1 AND 500),
    allow_multi  boolean     NOT NULL DEFAULT false,
    closes_at    timestamptz,
    -- Poll snapshots can arrive after a later committed vote. Consumers use
    -- this monotonic value to reject those stale snapshots.
    revision     bigint      NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, message_id),
    FOREIGN KEY (workspace_id, message_id)
        REFERENCES messages (workspace_id, message_id) ON DELETE CASCADE
);

CREATE TABLE message_poll_options (
    workspace_id uuidv7 NOT NULL,
    message_id   uuidv7 NOT NULL,
    option_id    uuidv7 NOT NULL,
    text         text   NOT NULL CHECK (length(text) BETWEEN 1 AND 200),
    ord          int    NOT NULL CHECK (ord >= 0),
    PRIMARY KEY (workspace_id, option_id),
    UNIQUE (workspace_id, message_id, option_id),
    UNIQUE (workspace_id, message_id, ord),
    FOREIGN KEY (workspace_id, message_id)
        REFERENCES message_polls (workspace_id, message_id) ON DELETE CASCADE
);

CREATE TABLE message_poll_votes (
    workspace_id uuidv7      NOT NULL,
    message_id   uuidv7      NOT NULL,
    option_id    uuidv7      NOT NULL,
    voter_kind   text        NOT NULL
        CHECK (voter_kind IN ('human', 'personality_agent')),
    voter_id     uuidv7      NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, option_id, voter_kind, voter_id),
    FOREIGN KEY (workspace_id, message_id)
        REFERENCES message_polls (workspace_id, message_id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id, message_id, option_id)
        REFERENCES message_poll_options (workspace_id, message_id, option_id) ON DELETE CASCADE
);

CREATE INDEX message_poll_votes_by_poll
    ON message_poll_votes (workspace_id, message_id, created_at);
CREATE INDEX message_poll_options_by_poll
    ON message_poll_options (workspace_id, message_id, ord);

-- A poll-only message is legitimate. Keep the existing deferred invariant but
-- admit either kind of message attachment in the same transaction.
CREATE OR REPLACE FUNCTION require_attachment_for_empty_message()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.deleted_at IS NULL AND NEW.content = ''
       AND NOT EXISTS (
           SELECT 1 FROM message_attachments a
           WHERE a.workspace_id = NEW.workspace_id
             AND a.place_id = NEW.place_id
             AND a.message_id = NEW.message_id
       )
       AND NOT EXISTS (
           SELECT 1 FROM message_polls p
           WHERE p.workspace_id = NEW.workspace_id
             AND p.message_id = NEW.message_id
       ) THEN
        RAISE EXCEPTION 'a message with empty content must bind an attachment or poll';
    END IF;
    RETURN NULL;
END;
$$;
