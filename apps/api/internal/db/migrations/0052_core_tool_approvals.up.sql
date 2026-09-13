-- Durable tool-call approval state (ADR 0013, M08 tools/authority slice).
--
-- A recorded planned call that requires human approval parks at the
-- operation ledger: the operation waits in 'awaiting_approval', the turn in
-- 'awaiting', and the input in 'waiting'. The approval row carries the exact
-- action (tool + route + request digest), the authority provenance granted,
-- and the authenticated human's one-shot decision — approve_once consumes
-- once into exactly-one execution, deny_once finalizes the operation failed
-- and is never silently retried or bypassed.

ALTER TABLE core_inputs DROP CONSTRAINT core_inputs_status_check;
ALTER TABLE core_inputs ADD CONSTRAINT core_inputs_status_check
    CHECK (status IN ('queued','claimed','waiting','done'));

ALTER TABLE core_turns DROP CONSTRAINT core_turns_status_check;
ALTER TABLE core_turns ADD CONSTRAINT core_turns_status_check
    CHECK (status IN ('running','awaiting','done','interrupted','failed'));

ALTER TABLE core_operations DROP CONSTRAINT core_operations_status_check;
ALTER TABLE core_operations ADD CONSTRAINT core_operations_status_check
    CHECK (status IN ('running','awaiting_approval','done','failed'));

CREATE TABLE core_tool_approvals (
    approval_id     text        NOT NULL,
    persona_id      uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    input_id        text        NOT NULL,
    call_index      int         NOT NULL,
    operation_id    text        NOT NULL,
    turn_id         text        NOT NULL,
    tool            text        NOT NULL,
    -- The invocation route recorded in the durable plan ('normal' |
    -- 'elevated'); immutable for the life of the request.
    route           text        NOT NULL CHECK (route IN ('normal','elevated')),
    -- Why approval was required: the tool's intrinsic registration
    -- ('intrinsic') or the model's explicit elevated route ('route').
    required_by     text        NOT NULL CHECK (required_by IN ('intrinsic','route')),
    request         jsonb       NOT NULL,
    -- Domain-separated sha256 over tool/route/canonical request — the exact
    -- action the human decided on.
    action_digest   text        NOT NULL,
    status          text        NOT NULL CHECK (status IN ('pending','approved','denied')),
    -- CurrentCallDecision vocabulary only (ADR 0013 §5): standing policy
    -- changes are a separate authenticated command and are not stored here.
    decision        text        CHECK (decision IN ('approve_once','deny_once')),
    -- The authenticated decision command's id — replays of the same command
    -- are idempotent, a different decision on a resolved approval conflicts.
    decision_id     text,
    decided_by_kind text,
    decided_by_id   text,
    -- ExecutionAuthorityProvenance resolved for the grant. Only approvals
    -- carry one; this slice grants 'agent_own_with_human_consent'.
    provenance      text        CHECK (provenance IN ('agent_own_with_human_consent')),
    decided_at      timestamptz,
    -- One-shot consumption: set by the claim transaction that executes the
    -- approved call, atomically with the effect.
    consumed_at     timestamptz,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (persona_id, approval_id),
    UNIQUE (persona_id, input_id, call_index)
);
CREATE INDEX core_tool_approvals_status ON core_tool_approvals(persona_id, status);
