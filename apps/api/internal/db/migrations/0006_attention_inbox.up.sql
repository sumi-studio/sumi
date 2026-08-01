-- 0006_attention_inbox: the per-agent AttentionCandidate inbox
-- (docs/messaging-boundary-contract.md 凍結v1, ADR 0011 §8-§11).
--
-- The shared control plane is the canonical holder of candidates: only the
-- shared side can receive while the agent runtime is stopped. The runtime's
-- own queue is a projection. Candidates are issued in the same transaction as
-- the message commit (the inbox is its own outbox), delivered at-least-once
-- ordered by candidate_seq, and deduplicated by candidate_id on the consumer
-- side.
--
-- What the boundary evaluates here is 権限と安全 plus the owner's standing
-- instructions — never "should this agent care" (ADR 0011 §8: the gate
-- executes the person's instruction, it does not judge).

-- 1. attention_cursors — one row per agent: the candidate_seq allocator and
--    the delivery cursor. acked_seq is monotonic and can never pass what was
--    issued.
CREATE TABLE attention_cursors (
    personality_agent_id uuidv7      PRIMARY KEY REFERENCES agents(personality_agent_id),
    issued_seq           bigint      NOT NULL DEFAULT 0
        CHECK (issued_seq >= 0 AND issued_seq <= 9007199254740991),
    acked_seq            bigint      NOT NULL DEFAULT 0
        CHECK (acked_seq >= 0),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CHECK (acked_seq <= issued_seq)
);

-- 2. attention_inbox — the candidates. A candidate is a pointer to a delivered
--    message plus the trigger snapshot; it never carries message content (the
--    provenance/delivery data is assembled at send-to-runtime time). Ack moves
--    the cursor; resolution here records only what the shared side itself can
--    know: the place's read cursor passed the candidate (superseded — 既読が
--    追い越したらもう起こさない). Budget exhaustion never deletes rows.
CREATE TABLE attention_inbox (
    candidate_id         uuidv7      PRIMARY KEY,
    personality_agent_id uuidv7      NOT NULL REFERENCES agents(personality_agent_id),
    candidate_seq        bigint      NOT NULL
        CHECK (candidate_seq > 0 AND candidate_seq <= 9007199254740991),
    place_id             uuidv7      NOT NULL REFERENCES places(place_id),
    message_id           uuidv7      NOT NULL REFERENCES messages(message_id),
    message_seq          bigint      NOT NULL CHECK (message_seq > 0),
    trigger_reason       text        NOT NULL
        CHECK (trigger_reason IN ('mention', 'keyword', 'dm', 'direct_call', 'all')),
    urgency              text        NOT NULL
        CHECK (urgency IN ('urgent', 'normal', 'fyi')),
    unread_from          bigint      NOT NULL CHECK (unread_from > 0),
    unread_to            bigint      NOT NULL,
    arrival_time         timestamptz NOT NULL DEFAULT now(),
    resolved_at          timestamptz,
    resolution           text
        CHECK (resolution IN ('superseded')),
    CHECK (unread_to >= unread_from - 1),
    CHECK ((resolved_at IS NULL) = (resolution IS NULL)),
    UNIQUE (personality_agent_id, candidate_seq)
);

CREATE INDEX attention_inbox_pending
    ON attention_inbox (personality_agent_id, candidate_seq)
    WHERE resolved_at IS NULL;

CREATE INDEX attention_inbox_by_place
    ON attention_inbox (personality_agent_id, place_id)
    WHERE resolved_at IS NULL;
