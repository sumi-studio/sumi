-- A response can be lost after a channel, copy, or group DM commits. The
-- authenticated actor's nonce records the exact creation result so a retry
-- returns that place rather than creating a second one.
CREATE TABLE messaging_place_creation_receipts (
    workspace_id   uuidv7 NOT NULL,
    member_kind    text   NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id      uuidv7 NOT NULL,
    operation      text   NOT NULL
        CHECK (operation IN ('create_channel', 'duplicate_channel', 'create_group_dm')),
    client_nonce   text   NOT NULL CHECK (length(client_nonce) BETWEEN 1 AND 128),
    request_digest bytea  NOT NULL CHECK (octet_length(request_digest) = 32),
    place_id       uuidv7 NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, member_kind, member_id, operation, client_nonce),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id)
);
