-- 0031_thread_creation_receipts: a thread create is a durable operation, not
-- merely an insert. Keep the caller's nonce receipt so a lost response can be
-- retried without minting another thread.
CREATE TABLE thread_creation_receipts (
    workspace_id    uuidv7      NOT NULL,
    creator_kind    text        NOT NULL
        CHECK (creator_kind IN ('human', 'personality_agent')),
    creator_id      uuidv7      NOT NULL,
    client_nonce    text        NOT NULL
        CHECK (length(client_nonce) BETWEEN 1 AND 128),
    thread_id       uuidv7      NOT NULL,
    parent_place_id uuidv7      NOT NULL,
    parent_message_id uuidv7,
    name            text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, creator_kind, creator_id, client_nonce),
    UNIQUE (workspace_id, thread_id),
    FOREIGN KEY (workspace_id, thread_id)
        REFERENCES places (workspace_id, place_id)
);
