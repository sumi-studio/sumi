-- 0005_messaging_schema: the messaging surface tables (ADR 0011,
-- docs/messaging-contracts-draft.md). Humans and PersonalityAgents are the
-- same "participant" everywhere: author, membership, mention, and read marker
-- rows all carry a (kind, id) pair instead of referencing one identity table.
-- kind is a sum type that will grow "app" later; consumers must treat unknown
-- kinds fail-closed.
--
-- Workspaces live here (not in the 戸籍): the 戸籍 records who exists, the
-- messaging schema records where they gather. employments.employer_id
-- ('workspace') from migration 0002 points at workspaces.workspace_id; the
-- application layer validates that reference, matching the comment in 0002.

-- 1. workspaces — the Discord-shaped server. Channels live directly under a
--    Workspace (Codex合意 v0: no nesting, no org concept).
CREATE TABLE workspaces (
    workspace_id uuidv7      PRIMARY KEY,
    name         text        NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- 2. workspace_members — Humans and PersonalityAgents in the same shape, with
--    the same roles. v0 rule: every active member can read and post in every
--    public channel of the Workspace. Leaving closes the row (left_at) instead
--    of deleting it, so authorship history stays explainable.
CREATE TABLE workspace_members (
    workspace_member_id bigserial   PRIMARY KEY,
    workspace_id        uuidv7      NOT NULL REFERENCES workspaces(workspace_id),
    member_kind         text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id           uuidv7      NOT NULL,
    role                text        NOT NULL DEFAULT 'member'
        CHECK (role IN ('owner', 'admin', 'member')),
    joined_at           timestamptz NOT NULL DEFAULT now(),
    left_at             timestamptz,
    CHECK (left_at IS NULL OR left_at >= joined_at)
);

CREATE UNIQUE INDEX workspace_members_one_active_per_participant
    ON workspace_members (workspace_id, member_kind, member_id)
    WHERE left_at IS NULL;

-- 3. places — where messages flow: channel (Workspace child), dm (2 people),
--    group_dm (3+). Every place owns a monotonically increasing seq; unread,
--    replay, permalinks, and read markers are all defined against it. last_seq
--    is the allocator: message insert increments it in the same transaction.
--    The upper bound is the wire contract's JsonSafeInteger cap, matching
--    MAX_PROVENANCE_SEQ on the agent side.
CREATE TABLE places (
    place_id     uuidv7      PRIMARY KEY,
    kind         text        NOT NULL CHECK (kind IN ('channel', 'dm', 'group_dm')),
    workspace_id uuidv7      REFERENCES workspaces(workspace_id),
    name         text        CHECK (name IS NULL OR length(name) BETWEEN 1 AND 200),
    topic        text        NOT NULL DEFAULT '',
    -- private is reserved by the contract; v0 only creates public channels.
    visibility   text        NOT NULL DEFAULT 'public'
        CHECK (visibility IN ('public', 'private')),
    -- Canonical sorted participant key ("kind:id|kind:id") for dm places only:
    -- makes "one dm per pair" a database guarantee instead of a race.
    dm_key       text        UNIQUE,
    last_seq     bigint      NOT NULL DEFAULT 0
        CHECK (last_seq >= 0 AND last_seq <= 9007199254740991),
    created_at   timestamptz NOT NULL DEFAULT now(),
    -- Channels are Workspace-scoped and named; dm/group_dm are global and
    -- unnamed (display names are derived from participants).
    CHECK ((kind = 'channel') = (workspace_id IS NOT NULL)),
    CHECK ((kind = 'channel') = (name IS NOT NULL)),
    CHECK ((kind = 'dm') = (dm_key IS NOT NULL))
);

CREATE INDEX places_by_workspace ON places (workspace_id) WHERE workspace_id IS NOT NULL;

-- 4. place_members — participants of dm/group_dm places. Channel membership is
--    derived from workspace_members and is not duplicated here.
CREATE TABLE place_members (
    place_member_id bigserial   PRIMARY KEY,
    place_id        uuidv7      NOT NULL REFERENCES places(place_id),
    member_kind     text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id       uuidv7      NOT NULL,
    joined_at       timestamptz NOT NULL DEFAULT now(),
    left_at         timestamptz,
    CHECK (left_at IS NULL OR left_at >= joined_at)
);

CREATE UNIQUE INDEX place_members_one_active_per_participant
    ON place_members (place_id, member_kind, member_id)
    WHERE left_at IS NULL;

CREATE INDEX place_members_by_participant
    ON place_members (member_kind, member_id)
    WHERE left_at IS NULL;

-- 5. messages — the durable events of a place. seq is allocated from
--    places.last_seq inside the insert transaction, so (place_id, seq) is dense
--    and gapless per place. Deletion is a tombstone: content is nulled, the
--    fact and the seq remain (契約ドラフト v0.1). client_nonce is the sender's
--    idempotency key: retrying a send returns the original receipt instead of
--    double-posting.
CREATE TABLE messages (
    message_id   uuidv7      PRIMARY KEY,
    place_id     uuidv7      NOT NULL REFERENCES places(place_id),
    seq          bigint      NOT NULL CHECK (seq > 0 AND seq <= 9007199254740991),
    author_kind  text        NOT NULL
        CHECK (author_kind IN ('human', 'personality_agent')),
    author_id    uuidv7      NOT NULL,
    -- NULL means tombstoned; the CHECK ties the two so a deleted message can
    -- never retain content and a live one can never lose it.
    content      text        CHECK (length(content) <= 65536),
    urgency      text        NOT NULL DEFAULT 'normal'
        CHECK (urgency IN ('urgent', 'normal', 'fyi')),
    reply_to     uuidv7      REFERENCES messages(message_id),
    client_nonce text        NOT NULL CHECK (length(client_nonce) BETWEEN 1 AND 128),
    created_at   timestamptz NOT NULL DEFAULT now(),
    edited_at    timestamptz,
    deleted_at   timestamptz,
    CHECK ((content IS NULL) = (deleted_at IS NOT NULL)),
    UNIQUE (place_id, seq)
);

CREATE UNIQUE INDEX messages_idempotent_send
    ON messages (place_id, author_kind, author_id, client_nonce);

-- 6. message_mentions — mention targets resolved at admission time from active
--    membership (raw @string matching is never used for authorization or
--    delivery decisions after this point). Kept relational so mention-unread
--    counts are one indexed query per participant.
CREATE TABLE message_mentions (
    message_id  uuidv7 NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    member_kind text   NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7 NOT NULL,
    PRIMARY KEY (message_id, member_kind, member_id)
);

CREATE INDEX message_mentions_by_participant ON message_mentions (member_kind, member_id);

-- 7. read_markers — participant × place read cursor. Monotonic: the store only
--    moves it forward (GREATEST on conflict), so a stale client can never
--    resurrect unread state. read_through(place, seq) is idempotent by
--    construction. No read receipts: this row is private input to unread
--    counts and attention supersession, never broadcast to other participants.
CREATE TABLE read_markers (
    place_id      uuidv7      NOT NULL REFERENCES places(place_id),
    member_kind   text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id     uuidv7      NOT NULL,
    last_read_seq bigint      NOT NULL
        CHECK (last_read_seq >= 0 AND last_read_seq <= 9007199254740991),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (place_id, member_kind, member_id)
);
