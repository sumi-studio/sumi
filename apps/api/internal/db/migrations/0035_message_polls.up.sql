-- 0035_message_polls: a poll is the one-to-one projection of its carrier
-- message. Options are server-minted UUIDv7 values in sender order; votes are
-- the complete visible choices of canonical Messaging participants.
CREATE TABLE message_polls (
    message_id  uuidv7      PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
    question    text        NOT NULL CHECK (char_length(question) BETWEEN 1 AND 500),
    allow_multi boolean     NOT NULL DEFAULT false,
    closes_at   timestamptz,
    revision    bigint      NOT NULL DEFAULT 0 CHECK (revision >= 0),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE message_poll_options (
    option_id  uuidv7   PRIMARY KEY,
    message_id uuidv7   NOT NULL REFERENCES message_polls(message_id) ON DELETE CASCADE,
    text       text     NOT NULL CHECK (char_length(text) BETWEEN 1 AND 200),
    ord        smallint NOT NULL CHECK (ord BETWEEN 0 AND 9),
    UNIQUE (message_id, ord),
    UNIQUE (message_id, text)
);

CREATE TABLE message_poll_votes (
    option_id  uuidv7      NOT NULL REFERENCES message_poll_options(option_id) ON DELETE CASCADE,
    voter_kind text        NOT NULL
        CHECK (voter_kind IN ('human', 'personality_agent')),
    voter_id   uuidv7      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (option_id, voter_kind, voter_id)
);

-- A poll-only message is legitimate. Keep the existing deferred invariant but
-- admit a poll inserted later in the same transaction as its carrier message.
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
           WHERE p.message_id = NEW.message_id
       ) THEN
        RAISE EXCEPTION 'a message with empty content must bind an attachment or poll';
    END IF;
    RETURN NULL;
END;
$$;
