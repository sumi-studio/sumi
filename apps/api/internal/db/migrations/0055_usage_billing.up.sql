-- Usage accounting and explicit budgets for the TypeScript core.
--
-- Every provider call the core makes is admitted against the funding
-- principal actually selected for that call, then recorded as an
-- immutable usage fact. Facts carry the funding identity captured at call
-- time (a later connection switch never reattributes earlier calls), keep
-- provider-reported token categories distinct, and mark whether usage was
-- reported by the provider or is unknown (a lost or refused response is
-- recorded as 'unknown', never silently zero).
--
-- Money is integer minor units with an explicit currency and the pricing
-- revision that produced the estimate. Rates are configured per funding
-- source by its owner — they are declared estimates, never presented as a
-- provider bill. No floating-point money anywhere on this path.

-- usage_funding_grants: a Sumi-provided allocation granting one human's
-- personas spend on a funding id that is not their own connection. Rows
-- are created by placement administration only — there is no
-- self-service or key-lending path in this slice. 'connection' funding
-- needs no grant row: ownership is model_api_connections.human_id.
CREATE TABLE usage_funding_grants (
    funding_id text        NOT NULL PRIMARY KEY,
    human_id   uuidv7      NOT NULL REFERENCES humans(human_id),
    label      text        NOT NULL DEFAULT '',
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- usage_budgets: the owner's explicit spending cap on one funding source.
-- Absence of a row means uncapped — never an implicit default. Rates price
-- both admission reservations and recorded usage; they are required when
-- a limit is configured because a limit without a rate card cannot bound
-- spend. pricing_revision marks the rate card's provenance (e.g.
-- 'owner-entered 2026-09', 'fixture-rates-v1' in tests).
CREATE TABLE usage_budgets (
    -- 'operator' budgets are operator-set (SetBudgetAdmin); humans manage
    -- only 'connection' rows, and 'sumi' caps belong to the funder.
    funding_kind         text NOT NULL CHECK (funding_kind IN ('connection','sumi','operator')),
    funding_id           text NOT NULL,
    limit_minor          bigint NOT NULL CHECK (limit_minor >= 0),
    currency             text NOT NULL CHECK (char_length(currency) = 3),
    rate_input_per_mtok  bigint NOT NULL CHECK (rate_input_per_mtok >= 0),
    rate_output_per_mtok bigint NOT NULL CHECK (rate_output_per_mtok >= 0),
    -- Cached input tokens bill at this rate when set, else the input rate.
    rate_cached_per_mtok bigint CHECK (rate_cached_per_mtok >= 0),
    pricing_revision     text NOT NULL DEFAULT '',
    updated_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (funding_kind, funding_id)
);

-- usage_reservations: spend held by an admitted call before its fact
-- lands. 'held' rows count against the budget until the call's fact
-- settles them; a call whose record never arrives is NOT released back to
-- the allowance — turn commit / generation recovery writes an inspectable
-- 'unrecorded' fact carrying the estimate as explicitly estimated spend.
-- Only a fact reporting 'not_sent' (the core asserts no request was
-- produced) releases the hold.
--
-- The row also snapshots the admission-time pricing provenance (rate card,
-- currency, revision) and the call's identity (kind/input/round/funding):
-- the fact is priced under the card that admitted the call, so a mid-call
-- rate, currency or budget change cannot rewrite that call's cost basis,
-- and a lost record can still be reconstructed as an 'unrecorded' fact.
CREATE TABLE usage_reservations (
    persona_id     uuidv7 NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    fact_id        text   NOT NULL,
    kind           text   NOT NULL,
    phase          text   NOT NULL,
    turn_id        text,
    input_id       text,
    round          int,
    funding_kind   text   NOT NULL,
    funding_id     text   NOT NULL,
    funding        jsonb  NOT NULL,
    reserved_minor bigint NOT NULL CHECK (reserved_minor >= 0),
    currency       text,
    -- bounded=false: the call's output was not limited at admission, so
    -- the reservation bounds admission, not the call's external bill.
    bounded        boolean NOT NULL DEFAULT false,
    -- Admission-time estimate and the rate card that priced it. NULL rate
    -- columns mean the call was admitted under no configured budget.
    est_input_tokens     bigint,
    est_output_bound     bigint,
    rate_input_per_mtok  bigint,
    rate_output_per_mtok bigint,
    rate_cached_per_mtok bigint,
    pricing_revision     text,
    generation     bigint NOT NULL,
    status         text   NOT NULL CHECK (status IN ('held','settled','released')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    settled_at     timestamptz,
    PRIMARY KEY (persona_id, fact_id)
);
CREATE INDEX usage_reservations_held
    ON usage_reservations (funding_kind, funding_id) WHERE status = 'held';

-- usage_facts: the ledger of recorded work. One row per logical provider
-- call — a redelivery of the same fact_id replays the stored row; a
-- genuinely additional call carries a different fact_id. Token columns are
-- NULL when the provider did not report that category; quantities keeps
-- the raw provider report for provenance. cost_minor is NULL when the fact
-- could not be priced (no rate card, or a provably-unsent call).
--
-- status:
--   reported   — the provider's own usage report resolved. cost_minor is
--                priced under the admission-snapshot rate card.
--   unknown    — the call was attempted but no complete usage report
--                resolved. Categories the provider did report are kept;
--                the admission estimate is retained as cost_minor with
--                cost_basis 'admission_estimate' — uncertain spend stays
--                spent; a later complete 'reported' for the same fact_id
--                upgrades the row to the provider's actual quantities.
--                'reported' requires both input and output tokens.
--   not_sent   — the core asserts no request was produced after admission.
--                The reservation is released; nothing was or can be owed.
--   unrecorded — admitted, then the record never landed (lost response,
--                dead writer). Written by reservation reconciliation with
--                the estimate retained as 'admission_estimate' spend.
--
-- Normalized token columns never overlap: input_tokens excludes
-- cached_tokens regardless of the provider's wire convention (chat/
-- responses report input including the cached subset; Anthropic reports
-- cache read/write additively — cache-write input is folded into
-- input_tokens and priced at the input rate).
CREATE TABLE usage_facts (
    persona_id    uuidv7 NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    fact_id       text   NOT NULL,
    kind          text   NOT NULL,  -- 'model_call' (extensible: 'execution')
    phase         text   NOT NULL,  -- 'turn' | 'memory'
    turn_id       text,
    input_id      text,
    round         int,
    funding_kind  text   NOT NULL CHECK (funding_kind IN ('connection','operator','sumi')),
    funding_id    text   NOT NULL,
    -- Non-secret snapshot of the selected funding identity at call time:
    -- connection version/model/preset, or the env fallback provider name.
    funding       jsonb  NOT NULL,
    status        text   NOT NULL CHECK (status IN ('reported','unknown','not_sent','unrecorded')),
    input_tokens  bigint,
    output_tokens bigint,
    cached_tokens bigint,
    quantities    jsonb  NOT NULL DEFAULT '{}',
    cost_minor    bigint,
    currency      text,
    -- 'configured_rates' when priced from the owner's rate card.
    cost_basis    text,
    pricing_revision text,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (persona_id, fact_id)
);
CREATE INDEX usage_facts_funding
    ON usage_facts (funding_kind, funding_id, recorded_at);

-- core_budget_waits: an input parked because admission denied it. The
-- turn commits 'await' with the denied funding + needed amount; a budget
-- increase, budget removal, or funding (selection) change requeues it.
-- Like an approval wait, an 'awaiting' turn does not count as an attempt.
CREATE TABLE core_budget_waits (
    persona_id   uuidv7 NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    input_id     text   NOT NULL,
    turn_id      text   NOT NULL,
    funding_kind text   NOT NULL,
    funding_id   text   NOT NULL,
    needed_minor bigint NOT NULL,
    currency     text   NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (persona_id, input_id)
);
