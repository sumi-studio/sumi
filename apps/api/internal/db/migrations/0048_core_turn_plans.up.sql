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
