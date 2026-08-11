-- 0011_message_reactions: emoji reactions on messages (ADR 0011 §3,
-- docs/messaging-contracts-draft.md). Reacting is the same capability for
-- Humans and PersonalityAgents, so the row carries the shared participant
-- (kind, id) shape used by author, membership, mention, and read marker rows.
--
-- One row means "this participant currently reacts to this message with this
-- emoji". The primary key makes "at most one identical reaction per person"
-- a database guarantee, so the store-level toggle can never duplicate rows
-- no matter how requests race or retry.
CREATE TABLE message_reactions (
    message_id  uuidv7      NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    member_kind text        NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id   uuidv7      NOT NULL,
    -- One emoji grapheme cluster; complex ZWJ sequences stay well within 32
    -- characters. The store validates shape, the schema bounds size.
    emoji       text        NOT NULL CHECK (length(emoji) BETWEEN 1 AND 32),
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id, member_kind, member_id, emoji)
);

-- A toggle is otherwise not retry-safe: losing the response and repeating the
-- same request would flip the state twice. Keep one durable result per acting
-- participant and client operation so Human and PersonalityAgent transports
-- can both retry an indeterminate request without changing the result again.
CREATE TABLE message_reaction_mutations (
    workspace_id uuidv7  NOT NULL,
    member_kind  text    NOT NULL
        CHECK (member_kind IN ('human', 'personality_agent')),
    member_id    uuidv7  NOT NULL,
    client_nonce text    NOT NULL CHECK (length(client_nonce) BETWEEN 1 AND 128),
    message_id   uuidv7  NOT NULL,
    emoji        text    NOT NULL CHECK (length(emoji) BETWEEN 1 AND 32),
    reacted      boolean NOT NULL,
    PRIMARY KEY (workspace_id, member_kind, member_id, client_nonce),
    FOREIGN KEY (workspace_id, message_id)
        REFERENCES messages (workspace_id, message_id) ON DELETE CASCADE
);
