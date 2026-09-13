# Core jobs: secretary-independent background executions

A **job** is a durable, persona-scoped execution record in `core_jobs`
(migration 0048, engineering plan §5 / work package M09). It exists so a
Linux command or other background task can keep running while the secretary
process stops, and its result reaches the *same* secretary — one continuing
individual — after it resumes.

## Ownership model

Three separate authorities, deliberately not conflated:

- **Writer generation** — the secretary's lease; gates secretary state
  mutations only. A job is *not* fenced by it: the writer fence must not
  revoke a legitimate running job's completion authority.
- **Runner claim** — `claimed_by` + `claim_expires_at` on the job row. A
  runner (a separate process, never a second personality) owns execution
  and heartbeats its claim. Expiry means *indeterminate*, not *retryable*.
- **Job identity** — server-derived for tool-started jobs:
  `op:<input_id>:<call_index>` (the plan position). The model can never
  choose an idempotency key, so a replayed claim can never mint a second
  job. Direct API submissions carry a caller-chosen `job_id` with
  identical-replay / divergent-conflict semantics.

## Lifecycle

```
queued ──claim──> running ──complete──> done | failed
   │                │
   │                ├──cancel──> cancel_requested ──complete──> cancelled | done | failed
   │                │                                    (runner reports what it observed)
   └──cancel──> cancelled                              │
running/cancel_requested + expired claim ──sweep──> lost
```

- `queued` jobs wait for a runner claim.
- `cancel_requested` is a transit state: the owning runner sees it via
  heartbeat, stops the process (SIGTERM → SIGKILL), and completes with the
  outcome it actually observed — `cancelled`, or the real exit result if
  the command had already finished (`cancel_requested_at` stays recorded).
- `lost` is terminal and honest: the runner's claim expired without a
  completion, so the subprocess outcome is unknown. The job is never
  silently re-executed.
- Every terminal transition enqueues exactly one notification input
  `job:<job_id>` (kind `job_completed`) in the same transaction.

## Result delivery

Completion is atomic with notification: `completeJob` writes the terminal
row and the `job:<job_id>` input in one transaction — the result is durable
before it is observable. The input enters the secretary's ordinary queue and
is claimed by the next `loadTurn` of the *same* persona — whether that
secretary process is the same one, a restart, or a new generation. The input
is consumed exactly once (durable input claim); replayed completes cannot
enqueue a second copy (`ON CONFLICT DO NOTHING` on the reserved id).

- Identical `complete` resend → `200` + stored row (lost-response path).
- Divergent `complete` → `409` + stored row (contract violation).
- `complete` from a non-owning runner → `409`.

## HTTP surface (persona-token scoped, not writer-gated)

```
POST   /internal/core/personas/{p}/jobs                 submit (idempotent on job_id)
GET    /internal/core/personas/{p}/jobs[?status=…&limit=…]
GET    /internal/core/personas/{p}/jobs/{id}
POST   /internal/core/personas/{p}/jobs/claim           {runner_id, kinds, lease_ms, limit}
POST   /internal/core/personas/{p}/jobs/{id}/heartbeat  {runner_id, lease_ms} → row (incl. cancel_requested)
POST   /internal/core/personas/{p}/jobs/{id}/cancel
POST   /internal/core/personas/{p}/jobs/{id}/complete   {runner_id, status, result, error}
```

`claim` also sweeps expired claims to `lost` + notification. `heartbeat` and
`complete` return `409` carrying the stored row when the caller no longer
owns the job, so a runner can stop the execution it still holds.

## Model-facing tools (state-internal)

`job.start`, `job.status`, `job.cancel` are registered internal tools: their
effects apply atomically inside the plan-bound operation claim transaction —
there is no crash window between the job row and its receipt. `job.start`
returns the `job` record (including the derived `job_id`) and the result
arrives later as a `job_completed` input; the turn does not wait on it.

The `job:` input prefix and the `op:` job-id prefix are reserved at the
submit boundary.

## Local runner

`apps/core/src/jobs/runner.ts` (`JobRunner`) +
`apps/core/src/host/job-runner.ts` (process entrypoint, `dev:job-runner`).

- Claims `subprocess` jobs, spawns `command[0]` with argv — never a shell.
- `cwd` resolves inside `SUMI_WORKSPACE_ROOT` and cannot escape it.
- Child env is minimal (`PATH`, `HOME`, `LANG`) + `request.env` + runner
  `baseEnv` — the child does not inherit service tokens from the runner.
- stdout/stderr are bounded (`SUMI_JOB_MAX_OUTPUT_BYTES`, default 256 KiB,
  truncation flagged).
- Heartbeats at ~lease/3; on `cancel_requested` SIGTERMs then SIGKILLs; on a
  `409` (terminal/lost/claimed-away) kills the child and leaves the stored
  verdict.
- Completion retries the identical body on transient failure; a `409` means
  the record already settled — the runner accepts it.
- Graceful `stop()` records `failed: runner stopped` for jobs it killed —
  a determinate outcome it knows. A `kill -9` crash records nothing; the
  claim expires and the job is swept to `lost`.

## Honest limits

- **Not a sandbox.** A subprocess runs with the runner process's privileges.
  There is no container, no network isolation, no resource limits beyond the
  timeout. Untrusted command execution needs the isolated-runtime slice.
- **At-least-recorded, not exactly-once external effects.** The record,
  notification, and dedup are exactly-once; a job's *effect on the world*
  may have happened once when the job is `lost` — that is why `lost` exists
  instead of silent re-execution.
- **Delivery, not read-receipt.** A `job_completed` input `done` means the
  secretary processed the notification turn, nothing more.
- **No workerd runner yet.** The Durable Object host reuses the same
  claim/heartbeat/complete contract; a DO-backed executor is a later slice.
- **Single-notification guarantee is durable**, not observational: the
  input row's `(persona_id, input_id)` uniqueness is what enforces it.
