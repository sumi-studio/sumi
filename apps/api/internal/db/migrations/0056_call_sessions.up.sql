-- 0056_call_sessions: durable call-participation sessions for the new core.
--
-- A call session is the authority record for one PersonalityAgent's presence
-- in one place's LiveKit room. The secretary's durable core creates a session
-- through the delegated call.join effect; a per-placement media bridge claims
-- it here, mints a room ticket only while its claim is live, joins under an
-- epoch-tagged identity, and reports participant status and utterance
-- dispositions back through persona-scoped routes.
--
-- Epoch bumps on every (re)claim so a bridge that loses its claim cannot
-- mint or act with stale authority: its LiveKit identity carries the epoch it
-- was issued under, letting the API remove a stale-epoch participant without
-- racing the current one.

CREATE TABLE call_sessions (
    session_id           uuidv7      PRIMARY KEY,
    workspace_id         uuidv7      NOT NULL,
    place_id             uuidv7      NOT NULL,
    personality_agent_id uuidv7      NOT NULL
        REFERENCES agents (personality_agent_id),
    -- LiveKit's stable room id, bound on first claim (a room may outlive the
    -- session record's request row by the webhook's delivery order).
    room_sid             text,
    -- requested:   created by call.join, no claim yet.
    -- claimed:     a bridge holds the claim; media setup in progress.
    -- active:      the bridge reports the participant connected.
    -- ending:      orderly shutdown requested (call.leave); the claim holder
    --              performs the disconnect and reports ended.
    -- ended:       terminal; no media authority remains.
    -- interrupted: claim lapsed while live; reclaimable — the next claim
    --              bumps the epoch and rejoins. Never terminal on its own.
    -- revoked:     terminal; membership/admission was withdrawn — the
    --              participant was removed server-side.
    -- failed:      terminal; the claim holder reported an unrecoverable
    --              media failure.
    status               text        NOT NULL
        CHECK (status IN ('requested','claimed','active','ending',
                          'ended','interrupted','revoked','failed')),
    -- Monotone claim generation. Every claim increments it; tickets minted
    -- and statuses reported carry the epoch they were authorized under.
    epoch                bigint      NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    claimed_by           text,
    claim_expires_at     timestamptz,
    -- How the session came to exist: 'call.join' (the secretary chose to
    -- join) or 'call_started' (reserved for an auto-join policy surface).
    requested_by         text        NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    ended_at             timestamptz,
    end_reason           text,
    FOREIGN KEY (workspace_id, place_id) REFERENCES places (workspace_id, place_id)
);

-- One live session per secretary per place. A second call.join to the same
-- place returns the live session rather than forking the room presence.
CREATE UNIQUE INDEX call_sessions_one_live_per_place
    ON call_sessions (personality_agent_id, place_id)
    WHERE status IN ('requested','claimed','active','ending','interrupted');

CREATE INDEX call_sessions_claimable
    ON call_sessions (personality_agent_id, claim_expires_at)
    WHERE status IN ('requested','claimed','active','ending','interrupted');

-- The secretary's committed speech. A row records intent, never audibility:
-- the bridge moves the row through dequeued → emitting → emitted and a crash
-- leaves an honest 'unknown'/'interrupted', so journal history can say what
-- was intended and what is known without claiming a listener heard it.
CREATE TABLE call_utterances (
    utterance_id  uuidv7      PRIMARY KEY,
    session_id    uuidv7      NOT NULL
        REFERENCES call_sessions (session_id) ON DELETE CASCADE,
    -- Session epoch at commit. A (re)claim bumps the session epoch and marks
    -- every non-terminal utterance from earlier epochs 'unknown' — committed
    -- intent is durable but never auto-replayed after a bridge restart.
    session_epoch bigint      NOT NULL CHECK (session_epoch >= 0),
    seq           bigint      NOT NULL CHECK (seq > 0),
    text          text        NOT NULL CHECK (length(text) BETWEEN 1 AND 4000),
    status        text        NOT NULL
        CHECK (status IN ('intended','dequeued','emitting','emitted',
                          'interrupted','expired','failed','unknown')),
    -- Disposition detail the bridge reports: fraction emitted, interrupt
    -- cause, failure detail. Nullable; semantics live in the status.
    detail        jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_id, seq)
);

CREATE INDEX call_utterances_pending
    ON call_utterances (session_id, seq)
    WHERE status = 'intended';
