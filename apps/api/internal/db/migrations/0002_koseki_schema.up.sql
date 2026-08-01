-- 0002_koseki_schema: the 戸籍 (identity registry) tables (ADR 0009).
-- Canonical terms live in CONTEXT.md. IDs are UUIDv7 stored as canonical
-- lowercase hyphenated text so the Go layer (ValidatePersonalityAgentID) and
-- the database enforce the identical format.

-- UUIDv7 domain: version nibble 7, RFC 4122 variant, lowercase hex. Matches
-- agentevents.ValidatePersonalityAgentID exactly. HumanId and PersonalityAgentId
-- share the same format (ADR 0009 §1).
CREATE DOMAIN uuidv7 AS text
CHECK (VALUE ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$');

-- 1. humans — the Human identity ledger. HumanId is minted once by the trusted
--    provisioning boundary and never changes.
CREATE TABLE humans (
    human_id     uuidv7     PRIMARY KEY,
    display_name text       NOT NULL DEFAULT 'Sumi',
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- 2. credentials — external login means bound permanently to one Human. A
--    credential is not an identity (ADR 0009 §2): one Human may hold several,
--    but a single credential is forever bound to the Human it was first bound
--    to. Rebinding to a different Human is forbidden by a trigger below.
CREATE TABLE credentials (
    credential_id     bigserial    PRIMARY KEY,
    provider          text         NOT NULL,
    external_subject  text         NOT NULL,
    human_id          uuidv7       NOT NULL REFERENCES humans(human_id),
    bound_at          timestamptz  NOT NULL DEFAULT now(),
    UNIQUE (provider, external_subject)
);

-- Prevent rebinding a credential to a different Human. This is the DB-level
-- guarantee that one credential maps to exactly one Human for all time.
CREATE OR REPLACE FUNCTION prevent_credential_rebinding() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.human_id IS DISTINCT FROM OLD.human_id THEN
        RAISE EXCEPTION 'credential is permanently bound to one Human and cannot be rebound';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER credential_no_rebind
    BEFORE UPDATE OF human_id ON credentials
    FOR EACH ROW
    EXECUTE FUNCTION prevent_credential_rebinding();

-- 3. agents — PersonalityAgent entities (ADR 0008). human_id is the Human whose
--    personal Secretary this agent is (stable across 異動). warmth is the
--    Employer cost setting, not a personality state (ADR 0010 §4).
CREATE TABLE agents (
    personality_agent_id uuidv7      PRIMARY KEY,
    human_id             uuidv7      NOT NULL REFERENCES humans(human_id),
    display_name         text        NOT NULL DEFAULT 'Sumi',
    warmth               text        NOT NULL DEFAULT 'cold'
        CHECK (warmth IN ('cold', 'warm')),
    created_at           timestamptz NOT NULL DEFAULT now()
);

-- 4. employments — who employs an agent and when. Employer is polymorphic:
--    a Human or a Workspace/org (employer_type discriminates). employer_id has
--    no single FK because Workspace rows land in a later migration (#125/#130);
--    the application layer validates the reference for the active type. One
--    agent has at most one active Employer at a time (ADR 0009 §4), enforced by
--    the partial unique index below. 異動 closes the current row and opens a
--    new one, preserving identity and history continuity.
CREATE TABLE employments (
    employment_id  bigserial    PRIMARY KEY,
    agent_id       uuidv7       NOT NULL REFERENCES agents(personality_agent_id),
    employer_type  text         NOT NULL
        CHECK (employer_type IN ('human', 'workspace')),
    employer_id    uuidv7       NOT NULL,
    started_at     timestamptz  NOT NULL DEFAULT now(),
    ended_at       timestamptz,
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE UNIQUE INDEX employments_one_active_employer_per_agent
    ON employments (agent_id)
    WHERE ended_at IS NULL;

-- 5. research_consents — explicit 研究協力 opt-in for content-log access
--    (ADR 0009 §6). The default life-log is private; only registered Humans are
--    unsealed. One active consent per Human; revocation is recorded.
CREATE TABLE research_consents (
    consent_id  bigserial    PRIMARY KEY,
    human_id    uuidv7       NOT NULL REFERENCES humans(human_id),
    granted_at  timestamptz  NOT NULL DEFAULT now(),
    revoked_at  timestamptz,
    CHECK (revoked_at IS NULL OR revoked_at >= granted_at)
);

CREATE UNIQUE INDEX research_consents_one_active_per_human
    ON research_consents (human_id)
    WHERE revoked_at IS NULL;
