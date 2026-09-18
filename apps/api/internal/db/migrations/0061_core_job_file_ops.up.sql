-- Job-scoped file operations: the durable ledger behind the script runner's
-- file capability. A script job never holds a file credential; it asks the
-- state service to perform a bounded, allowlisted filesvc operation under
-- the persona's derived scope, and every mutating operation is recorded
-- BEFORE the upstream call with a server-minted operation identity that is
-- also the filesvc X-Idempotency-Key. An operation whose outcome is not
-- known (lost response, transport failure, caller crash) stays 'admitted'
-- or 'unknown' — never assumed clean — until the keyed resend reconciles
-- it against the service's durable receipt.
--
-- Statuses: admitted (recorded, upstream outcome not yet known), settled
-- (upstream 2xx — executed or receipt replayed), refused (upstream 4xx —
-- determinate rejection, no effect), unknown (transport failure or 5xx —
-- effect indeterminate, reconcilable by keyed resend), diverged (keyed
-- resend returned filesvc's diverged verdict: accepted once, effect
-- unconfirmable — preserved, never retried into silence).
CREATE TABLE core_job_file_ops (
    persona_id  uuidv7      NOT NULL,
    job_id      text        NOT NULL,
    -- Per-job sequence, assigned under the job row lock so admitted
    -- operations have a stable durable order.
    op_seq      bigint      NOT NULL,
    -- Server-minted durable operation identity; also the exact
    -- X-Idempotency-Key sent to filesvc (charset/length per its
    -- validOpKey). Never caller-supplied.
    op_id       text        NOT NULL,
    op          text        NOT NULL CHECK (op IN ('write','mkdir','remove')),
    -- The derived filesvc scope the operation ran under (persona-derived,
    -- recorded for audit — authority comes from the persona, not this row).
    scope       text        NOT NULL,
    path        text        NOT NULL,
    -- Canonical request the key binds to: {if_version, body_sha256} — the
    -- resend must replay byte-identical semantics, so the write body is
    -- kept alongside for reconciliation.
    request     jsonb       NOT NULL,
    body        bytea,
    status      text        NOT NULL CHECK (status IN
        ('admitted','settled','refused','unknown','diverged')),
    result      jsonb,
    error       text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    resolved_at timestamptz,
    PRIMARY KEY (persona_id, job_id, op_seq),
    UNIQUE (op_id),
    FOREIGN KEY (persona_id, job_id) REFERENCES core_jobs(persona_id, job_id) ON DELETE CASCADE
);
-- Reconciliation reads only unresolved rows.
CREATE INDEX core_job_file_ops_pending ON core_job_file_ops(persona_id, job_id)
    WHERE status IN ('admitted','unknown');
