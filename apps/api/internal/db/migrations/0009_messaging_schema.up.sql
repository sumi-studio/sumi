-- 0009_messaging_schema: Messaging-owned tables. Workspace identity,
-- membership, roles, invites, and app lifecycle are owned by migration 0008.

CREATE TABLE places (
    place_id     uuidv7      PRIMARY KEY,
    kind         text        NOT NULL CHECK (kind IN ('channel', 'dm', 'group_dm')),
    workspace_id uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    name         text        CHECK (name IS NULL OR length(name) BETWEEN 1 AND 200),
    topic        text        NOT NULL DEFAULT '',
    visibility   text        NOT NULL DEFAULT 'public'
        CHECK (visibility IN ('public', 'private')),
    -- A 1:1 DM is stable for one canonical ParticipantRef pair inside one
    -- Workspace. Membership tenures may close and reopen without minting a
    -- second conversation or losing the pair's history.
    dm_key       text,
    last_seq     bigint      NOT NULL DEFAULT 0
        CHECK (last_seq >= 0 AND last_seq <= 9007199254740991),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'channel') = (name IS NOT NULL)),
    CHECK ((kind = 'dm') = (dm_key IS NOT NULL)),
    UNIQUE (workspace_id, place_id),
    UNIQUE (workspace_id, dm_key)
);

CREATE INDEX places_by_workspace ON places (workspace_id, created_at, place_id);

-- A place membership is its own tenure and is also pinned to the exact
-- Workspace membership tenure that admitted it. Joining a Workspace never
-- implicitly creates or revives this row. visible_from_seq is 1 for channels
-- and explicit 1:1-DM re-admission (full same-pair history); a group-DM
-- re-admission records the place's then-current last_seq + 1.
CREATE TABLE place_members (
    place_member_id      uuidv7      PRIMARY KEY,
    workspace_id        uuidv7      NOT NULL,
    place_id            uuidv7      NOT NULL,
    workspace_member_id uuidv7      NOT NULL,
    member_kind         text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id           uuidv7      NOT NULL,
    visible_from_seq    bigint      NOT NULL DEFAULT 1
        CHECK (visible_from_seq > 0 AND visible_from_seq <= 9007199254740991),
    joined_at           timestamptz NOT NULL DEFAULT now(),
    left_at             timestamptz,
    CHECK (left_at IS NULL OR left_at >= joined_at),
    UNIQUE (place_id, place_member_id),
    FOREIGN KEY (workspace_id, place_id)
        REFERENCES places (workspace_id, place_id),
    FOREIGN KEY (workspace_id, workspace_member_id, member_kind, member_id)
        REFERENCES workspace_members
            (workspace_id, workspace_member_id, member_kind, member_id)
);

CREATE UNIQUE INDEX place_members_one_active_per_participant
    ON place_members (place_id, member_kind, member_id)
    WHERE left_at IS NULL;

CREATE INDEX place_members_by_participant
    ON place_members (member_kind, member_id, workspace_id, place_id)
    WHERE left_at IS NULL;

CREATE TABLE messages (
    message_id   uuidv7      PRIMARY KEY,
    place_id     uuidv7      NOT NULL REFERENCES places(place_id),
    seq          bigint      NOT NULL CHECK (seq > 0 AND seq <= 9007199254740991),
    author_kind  text        NOT NULL
        CHECK (author_kind IN ('human', 'personality_agent')),
    author_id    uuidv7      NOT NULL,
    content      text        CHECK (length(content) <= 65536),
    urgency      text        NOT NULL DEFAULT 'normal'
        CHECK (urgency IN ('urgent', 'normal', 'fyi')),
    reply_to     uuidv7,
    client_nonce text        NOT NULL CHECK (length(client_nonce) BETWEEN 1 AND 128),
    created_at   timestamptz NOT NULL DEFAULT now(),
    edited_at    timestamptz,
    deleted_at   timestamptz,
    CHECK ((content IS NULL) = (deleted_at IS NOT NULL)),
    UNIQUE (place_id, seq),
    UNIQUE (place_id, message_id),
    FOREIGN KEY (place_id, reply_to)
        REFERENCES messages (place_id, message_id)
);

CREATE UNIQUE INDEX messages_idempotent_send
    ON messages (place_id, author_kind, author_id, client_nonce);

CREATE TABLE message_mentions (
    message_id  uuidv7 NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    member_kind text   NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7 NOT NULL,
    PRIMARY KEY (message_id, member_kind, member_id)
);

CREATE INDEX message_mentions_by_participant ON message_mentions (member_kind, member_id);

CREATE TABLE read_markers (
    place_id        uuidv7      NOT NULL,
    place_member_id uuidv7      NOT NULL,
    last_read_seq   bigint      NOT NULL
        CHECK (last_read_seq >= 0 AND last_read_seq <= 9007199254740991),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, place_member_id),
    FOREIGN KEY (place_id, place_member_id)
        REFERENCES place_members (place_id, place_member_id)
);
