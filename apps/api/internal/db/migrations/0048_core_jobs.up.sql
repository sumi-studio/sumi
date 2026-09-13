-- Core jobs: secretary-independent background executions (engineering plan
-- §5, work package M09). A job belongs to the persona but NOT to the writer
-- generation: the writer fence must not revoke a legitimate running job's
-- completion authority, so job lifecycle is owned by a runner claim
-- (claimed_by + claim_expires_at) that is independent of core_writer_leases.
--
-- Lifecycle: queued → running → done|failed|cancelled. A running job whose
-- runner asks for cancel passes through cancel_requested first. A running
-- job whose claim expires without completion becomes 'lost': its outcome is
-- indeterminate and it is never silently re-executed. Every terminal
-- transition enqueues exactly one notification input 'job:<job_id>' in the
-- same transaction, so the secretary learns the result through its ordinary
-- input stream after any restart — without a second notification.
CREATE TABLE core_jobs (
    persona_id  uuidv7      NOT NULL REFERENCES core_personas(persona_id) ON DELETE CASCADE,
    job_id      text        NOT NULL,
    -- Executor family. 'subprocess' is a local command; later kinds (script,
    -- cloud linux) keep the same claim/complete/cancel contract.
    kind        text        NOT NULL,
    request     jsonb       NOT NULL,
    status      text        NOT NULL CHECK (status IN
        ('queued','running','cancel_requested','done','failed','cancelled','lost')),
    -- Runner claim: which executor currently owns the job and until when.
    -- claim_expires_at is a liveness bound only; an expired claim reconciles
    -- to 'lost' on the next claim pass, never to silent re-execution.
    claimed_by         text,
    claim_expires_at   timestamptz,
    -- Provenance: 'api' for direct submission, 'tool:<turn_id>:<call_index>'
    -- for a job started as a plan-bound tool effect.
    created_by         text        NOT NULL DEFAULT 'api',
    created_at         timestamptz NOT NULL DEFAULT now(),
    started_at         timestamptz,
    finished_at        timestamptz,
    cancel_requested_at timestamptz,
    -- Terminal record: exit_code, bounded stdout/stderr tails, truncation
    -- flags, spawn/timeout reasons. Shape is kind-defined.
    result             jsonb,
    error              text,
    -- Set when the terminal notification input was durably queued.
    notified_at        timestamptz,
    PRIMARY KEY (persona_id, job_id)
);
CREATE INDEX core_jobs_queued ON core_jobs(persona_id, created_at)
    WHERE status = 'queued';
CREATE INDEX core_jobs_claimed ON core_jobs(persona_id, claim_expires_at)
    WHERE status IN ('running','cancel_requested');
