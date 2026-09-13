-- Core state: canonical PostgreSQL continuity for the shared TypeScript
-- secretary core (engineering plan §3/§5, work package M02). Every table is
-- persona-scoped; no row mixes one secretary's timeline with another's.

CREATE TABLE core_personas (
    persona_id  uuidv7      PRIMARY KEY,
    human_id    uuidv7      REFERENCES humans(human_id),
    display_name text       NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Writer exclusion: one row per persona. generation is a fencing token that
-- increases on every successful acquire; state mutations must present the
-- current generation inside the same transaction to commit.
CREATE TABLE core_writer_leases (
    persona_id   uuidv7      PRIMARY KEY REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    generation   bigint      NOT NULL,
    holder_id    text        NOT NULL,
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL
);

-- Durable input inbox. Callers supply input_id; re-submitting the same
-- (persona_id, input_id) replays the stored row including its result.
-- Provenance and attention are contract-level fields so shared-channel
-- routing ("who this concerns / who should notice") does not need a later
-- retrofit: actor_* identifies who produced the input, source_surface where,
-- thread_id correlates a shared channel, attention is the delivery hint the
-- secretary's own judgement may override.
CREATE TABLE core_inputs (
    persona_id         uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    input_id           text        NOT NULL,
    kind               text        NOT NULL,
    payload            jsonb       NOT NULL,
    actor_kind         text        NOT NULL DEFAULT '',
    actor_id           text        NOT NULL DEFAULT '',
    source_surface     text        NOT NULL DEFAULT '',
    thread_id          text        NOT NULL DEFAULT '',
    occurred_at        timestamptz,
    attention          text        NOT NULL DEFAULT 'reply' CHECK (attention IN ('reply','observe','defer')),
    status             text        NOT NULL CHECK (status IN ('queued','claimed','done')),
    claimed_generation bigint,
    turn_id            text,
    created_at         timestamptz NOT NULL DEFAULT now(),
    done_at            timestamptz,
    -- Retry backoff: a retryable-failed input requeues with a future
    -- not_before so it cannot instantly reclaim the queue and starve
    -- later inputs or hammer the provider.
    not_before         timestamptz,
    PRIMARY KEY (persona_id, input_id)
);
CREATE INDEX core_inputs_pending ON core_inputs(persona_id, created_at)
    WHERE status = 'queued';

CREATE TABLE core_turns (
    persona_id  uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    turn_id     text        NOT NULL,
    input_id    text        NOT NULL,
    generation  bigint      NOT NULL,
    attempt     int         NOT NULL,
    status      text        NOT NULL CHECK (status IN ('running','done','interrupted','failed')),
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    output      jsonb,
    usage       jsonb,
    error       text,
    -- commit_request is the exact accepted CommitRequest for this turn, so a
    -- replayed commit can be verified as byte-identical rather than inferred
    -- from journaled side effects.
    commit_request jsonb,
    PRIMARY KEY (persona_id, turn_id),
    FOREIGN KEY (persona_id, input_id) REFERENCES core_inputs(persona_id, input_id)
);
CREATE UNIQUE INDEX core_turns_one_running
    ON core_turns(persona_id) WHERE status = 'running';
CREATE INDEX core_turns_by_input ON core_turns(input_id);

-- Append-only journal: the continuity substrate for one life across restarts.
CREATE TABLE core_events (
    persona_id uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    seq        bigint      NOT NULL,
    turn_id    text        NOT NULL,
    kind       text        NOT NULL,
    payload    jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (persona_id, seq)
);

-- Operation ledger for authorized side effects: claim before execute,
-- record the receipt, and reclaim instead of re-executing after a crash.
-- response carries the effect receipt (e.g. a file version token) so a crash
-- between an external effect and its recording reconciles here instead of
-- leaving an unrecorded effect. State-internal tools apply their effect in
-- the claim transaction itself, making record and effect atomic.
CREATE TABLE core_operations (
    persona_id      uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    operation_id    text        NOT NULL,
    turn_id         text        NOT NULL,
    tool            text        NOT NULL,
    idempotency_key text        NOT NULL,
    request         jsonb       NOT NULL,
    status          text        NOT NULL CHECK (status IN ('running','done','failed')),
    response        jsonb,
    claimed_generation bigint   NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    PRIMARY KEY (persona_id, operation_id),
    UNIQUE (persona_id, tool, idempotency_key)
);

-- Durable wake schedule; alarm/queue mechanisms are delivery only, never the
-- record. miss_policy is recorded per entry: the product-visible policy for a
-- schedule whose wake was missed (fire late, coalesce, expire, report missed)
-- is chosen per schedule and defaults to firing late.
CREATE TABLE core_schedules (
    persona_id         uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    schedule_id        text        NOT NULL,
    wake_at            timestamptz NOT NULL,
    payload            jsonb       NOT NULL,
    miss_policy        text        NOT NULL DEFAULT 'fire_late'
        CHECK (miss_policy IN ('fire_late','coalesce','expire','report_missed')),
    status             text        NOT NULL CHECK (status IN ('pending','claimed','fired','cancelled','expired')),
    claimed_generation bigint,
    created_at         timestamptz NOT NULL DEFAULT now(),
    fired_at           timestamptz,
    PRIMARY KEY (persona_id, schedule_id)
);
CREATE INDEX core_schedules_due ON core_schedules(wake_at)
    WHERE status = 'pending';

-- Committed outward-facing results. Delivery to messaging/browser surfaces
-- is a later milestone; this is the durable source it will read.
CREATE TABLE core_outbox (
    persona_id   uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    seq          bigint      NOT NULL,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz,
    PRIMARY KEY (persona_id, seq)
);
-- Durable turn plans (F1): the model's decision for an input is recorded
-- before any of its effects execute. Recovery continues the recorded plan
-- instead of re-planning from pre-effect context, so a retried attempt can
-- never duplicate or silently substitute an already-issued decision.
--
-- Keyed by input, not turn: attempts are per-turn, but the decision belongs
-- to the input's resolution lineage — attempt N replays attempt 1's plan.
-- turn_id/generation record which attempt authored it, for audit only.
CREATE TABLE core_turn_plans (
    persona_id uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    input_id   text        NOT NULL,
    turn_id    text        NOT NULL,
    generation bigint      NOT NULL,
    -- {"text": string, "calls": [{"call_id"?, "tool", "request"}], "usage": {}}
    plan       jsonb       NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (persona_id, input_id),
    FOREIGN KEY (persona_id, input_id) REFERENCES core_inputs(persona_id, input_id)
);
