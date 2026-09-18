-- One durable interactive terminal session per row. Same
-- claim-epoch fencing as core_jobs/call_sessions: a claimed session
-- carries (claimed_by, claim_expires_at); the sweeper moves an
-- expired claim to 'interrupted' — reclaimable, NOT silently dead —
-- and the next claim bumps epoch so only one writer can commit
-- input dispositions, output appends, and status at a time.
CREATE TABLE core_terminal_sessions (
    session_id     uuidv7 PRIMARY KEY,
    persona_id     uuidv7 NOT NULL REFERENCES core_personas (persona_id) ON DELETE CASCADE,
    name           text NOT NULL DEFAULT '' CHECK (char_length(name) <= 80),
    mode           text NOT NULL CHECK (mode IN ('pty')),
    backend        text NOT NULL CHECK (backend IN ('local', 'cloud')),
    -- Runtime binding, set by the claiming runner. Deterministic
    -- runtime op ids are derivable from session_id, so this column is
    -- observability, not authority.
    operation_id   text,
    status         text NOT NULL CHECK (status IN
        ('requested', 'claimed', 'active', 'ending', 'ended', 'interrupted', 'lost')),
    epoch          bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    claimed_by     text,
    claim_expires_at timestamptz,
    -- Exclusive human control lease: while set and unexpired,
    -- agent-originated input is refused at admission ('control_held')
    -- rather than queued and silently dropped.
    control_holder text CHECK (control_holder IS NULL OR control_holder = 'human'),
    control_until  timestamptz,
    requested_by   text NOT NULL CHECK (requested_by IN ('agent', 'human')),
    created_by     text NOT NULL,
    exit_code      integer,
    exit_signal    text,
    end_reason     text,
    -- Absolute output coordinates: output_bytes is the high-water
    -- cursor emitted by the runner; output_base is the earliest
    -- retained offset after pruning. A read cursor below output_base
    -- observes an explicit gap, never silent truncation.
    output_bytes   bigint NOT NULL DEFAULT 0 CHECK (output_bytes >= 0),
    output_base    bigint NOT NULL DEFAULT 0 CHECK (output_base >= 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    ended_at       timestamptz
);

CREATE INDEX idx_core_terminal_sessions_persona
    ON core_terminal_sessions (persona_id, created_at);
CREATE INDEX idx_core_terminal_sessions_claimable
    ON core_terminal_sessions (status, backend)
    WHERE status IN ('requested', 'interrupted');
CREATE INDEX idx_core_terminal_sessions_claim_expiry
    ON core_terminal_sessions (claim_expires_at)
    WHERE status IN ('claimed', 'active', 'ending') AND claimed_by IS NOT NULL;

-- Serialized input ledger. Accepted input is durable before it is
-- delivered; seq is the delivery order. A crash after possible
-- delivery marks the row 'unknown' — it is never re-sent, because a
-- terminal byte may already have taken effect.
CREATE TABLE core_terminal_inputs (
    input_id      uuidv7 PRIMARY KEY,
    session_id    uuidv7 NOT NULL REFERENCES core_terminal_sessions (session_id) ON DELETE CASCADE,
    session_epoch bigint NOT NULL,
    seq           bigint NOT NULL CHECK (seq > 0),
    kind          text NOT NULL CHECK (kind IN ('stdin', 'resize', 'signal', 'eof')),
    payload       jsonb NOT NULL,
    source        text NOT NULL CHECK (source IN ('agent', 'human')),
    status        text NOT NULL CHECK (status IN
        ('intended', 'dequeued', 'written', 'interrupted', 'expired', 'failed', 'unknown')),
    detail        jsonb,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_id, seq)
);

CREATE INDEX idx_core_terminal_inputs_pending
    ON core_terminal_inputs (session_id, session_epoch, seq)
    WHERE status IN ('intended', 'dequeued');

-- Bounded append-only scrollback served to humans and the secretary.
-- The owning runner appends 'data' chunks at their absolute
-- provisioner offsets (UNIQUE(session_id, base, kind) makes a replayed
-- drain idempotent) and inserts 'gap' rows when retained provisioner
-- output skipped ahead. Reader cursors are absolute byte offsets.
CREATE TABLE core_terminal_output (
    session_id uuidv7 NOT NULL REFERENCES core_terminal_sessions (session_id) ON DELETE CASCADE,
    seq        bigint NOT NULL CHECK (seq > 0),
    kind       text NOT NULL CHECK (kind IN ('data', 'gap')),
    base       bigint NOT NULL CHECK (base >= 0),
    gap_to     bigint CHECK (gap_to IS NULL OR gap_to >= base),
    data       bytea NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_id, seq),
    UNIQUE (session_id, base, kind)
);

CREATE INDEX idx_core_terminal_output_read
    ON core_terminal_output (session_id, seq);

-- Shared terminal surface: participant-scoped installation like
-- direct-chat (the human operates it in the shared workspace).
INSERT INTO app_catalog (app_id, display_name, workspace_owner_allowed, participant_owner_allowed)
VALUES ('terminal', 'Terminal', false, true);
