# Linux-on-demand jobs (Cloud `subprocess`)

A Cloud `subprocess` job is submitted through the existing `job.start` tool
and permission semantics, then executed only when needed: the API-side
`jobexec` driver claims it from `core_jobs` and maps it to one durable
process operation on the root runtime provisioner, which launches a single
ephemeral hardened container bind-mounted to the persona's canonical
verified files scope. When the command ends the environment is reclaimed —
the container is removed — while the result, bounded output, and every file
the job wrote remain durable and reachable through the normal file APIs.

Two durable gates guard execution, and they are named distinctly throughout:

- **Claim** — `ClaimJobs` transitions the row `queued → running` with a
  `claimed_by` runner identity and a lease, gated on the persona's
  `authority = 'active'`. This is the only path to a launch attempt.
- **Journal admission** — `StartProcess` writes the operation record under
  the process-store mutex. Only a journaled operation may ever reach a
  container launch.

A persona seal or transfer (`authority` leaving `active`) therefore stops
*new* work at the claim gate: queued rows are never claimed (both the
discovery queries and the claim predicate itself filter on `active`), so
nothing new reaches the driver or the journal. Work already journaled is
*admitted* execution — it may run to completion across the transfer; there
is deliberately no kill-on-transfer. Cancellation and foreign sweeps are a
different matter and are fenced by the tombstone protocol below.

```text
job.start ──► core_jobs (queued, kind=subprocess)
                 ▲ claim lease + heartbeat          ▲ job.status / job.cancel
 jobexec driver (in apps/api process, runner id "jobexec-docker")
                 │  reconcile-before-claim sweep
                 ▼  StartProcess {image:"job", workspace:"files-scope"}
 runtime provisioner (root daemon, unix socket)
                 │  sumi-files-check mount+UUID+scope → durable binding
                 ▼  docker create --network none --read-only --user 10002 …
 ghcr.io/sumi-studio/sumi-job:<pinned-revision>
                 │  bind <mountpoint>/<scope> → /workspace
                 │  bind /run/sumi/egress → /run/sumi/egress (opt-in, below)
                 ▼
 output → process journal → job.result (bounded), files → canonical scope
```

## Request shape

```json
{
  "command": ["executable", "arg"],
  "cwd": "optional/relative/path",
  "timeout_ms": 1000,
  "env": { "JOB_VAR": "value" },
  "backend": "cloud"
}
```

`backend` is `"cloud"`, `"local"`, or absent. Absent takes the deployment
default (`SUMI_JOBS_DEFAULT_BACKEND`); on a deployment with no configured
default the request stays unstamped and claims under the `local` side of
the claim predicate.

Submission validation (`validateJobRequest`) enforces the same bounds the
process contract enforces, so a validated spec always maps to a launchable
request:
command ≤129 entries / ≤32 KiB / NUL-free, executable ≤1024 chars, `cwd`
clean-relative without `..`, `timeout_ms` integer in (0, 3.6M], env ≤32
entries with `[A-Za-z_][A-Za-z0-9_]*` names ≤64 chars, values ≤4 KiB, ≤8
KiB total, and the backend-owned `PATH`/`HOME`/`LANG` names refused.

A submission that explicitly requests `backend:"cloud"` is **refused**
while no Cloud runner is wired — the store only accepts it after the
driver's readiness probe has passed. Queuing it would otherwise be a
silent forever-wait, since nothing else can claim cloud-stamped work.

## What a job may not do

The request can never name an image, a mount, a network, or a Docker flag.
`image` is the server-side selector `"job"` (pinned via
`SUMI_JOB_IMAGE_TAG`); `workspace` is the selector `"files-scope"` whose
path is resolved and verified only by the provisioner. Containers run with
`--network none` — the job process has no NIC, no route, and no DNS — plus
read-only rootfs, uid 10002, `cap-drop ALL`, `no-new-privileges`,
1 CPU / 384 MiB / 128 pids, a 32 MiB noexec `/tmp`, the verified scope
directory bind at `/workspace`, and — only when egress is configured — the
egress socket directory bind described below.

## Job egress (opt-in)

When the provisioner runs with `SUMI_JOB_EGRESS_DIR` set, job containers
get a confined path to the public network — enough for ordinary package
tools (`pip install`, `curl`, `git clone` over HTTPS) while keeping
`--network none`:

```text
job container (--network none)          deployment side
HTTP_PROXY=http://127.0.0.1:3128
        │  sumi-egress-bridge (image binary, loopback listener)
        ▼  splice per connection
/run/sumi/egress/proxy.sock  ──bind──►  sumi-egress-proxy ──► public TCP only
        (host dir bind-mounted)         (CONNECT :443, http :80; every
                                         request re-resolves the host and
                                         dials only public validated IPs)
```

- The proxy (`sumi-egress-proxy`, built into the provisioner image, run as
  the `job-egress-proxy` service) listens **only** on the unix socket — it
  has no TCP listener, no Docker socket, and no credentials, and in the
  reference compose it is the sole member of a dedicated `job-egress`
  bridge network (NAT egress, no control-plane service peers). The bridge
  itself is not an outbound firewall; destination checks remain in the proxy. For every
  request it resolves the destination fresh, requires every resolved
  address to satisfy `publicweb.IsPublicAddress`, and dials the validated
  IP literals — a DNS answer that turns private between requests is denied
  on the next request, and the dialed set is exactly the checked set.
  `CONNECT` is accepted for port 443 only; plain `http://` forwarding for
  port 80 only; redirects are returned to the client, never followed by
  the proxy. Everything else answers 403.
- The bridge (`sumi-egress-bridge`, built into the job image) is spawned
  by the launch wrapper under `setsid` — its own session and process
  group, still inside the job's PID namespace. Foreground-group signals
  (an idle-prompt Ctrl-C, the terminal's `SignalProcess` path) reach the
  shell and its jobs but never the bridge; container teardown still reaps
  it. A job that deliberately kills it only loses its own egress.
- `SUMI_JOB_EGRESS_DIR` is the host directory containing `proxy.sock`.
  `api-state-init` creates it owned by the proxy uid (mode 0755); the
  socket itself is mode 0622 — connect needs write on the socket file.
  The provisioner only names the directory to dockerd; it never opens
  the socket. Unset/empty `SUMI_JOB_EGRESS_DIR` keeps the launch spec
  byte-for-byte the old no-network contract, and the same wiring covers
  interactive terminal ops, so the shared terminal can install into the
  workspace too.
- A down or missing proxy degrades honestly: tools see connection refused
  and the job otherwise runs normally; workspace files are unaffected and
  nothing is retried automatically.
- Request-supplied `*_PROXY`/`NO_PROXY` variables are dropped when egress
  is configured — the backend owns the proxy environment. It injects a
  loopback-only bypass (`NO_PROXY=localhost,127.0.0.1,::1`) so a
  job-local dev server stays reachable by ordinary tools while every
  other destination still goes through the proxy. Egress-enabled job ops
  also get `PATH=/workspace/.local/bin:…` — and the job image's
  `/etc/profile.d` snippet restores it for `bash -l` login shells — so
  `pip --user` console scripts are runnable by name in the installing
  session and later ones.

## Job image

`deploy/job/Dockerfile` builds `ghcr.io/sumi-studio/sumi-job` —
debian-slim with bash/coreutils/findutils/grep/sed/gawk, make, gcc,
libc6-dev, python3 + pip, and curl, running as uid/gid 10002, plus the
`sumi-egress-bridge` binary and the `/run/sumi/egress` mountpoint.
`PIP_BREAK_SYSTEM_PACKAGES=1` is set image-wide because the container is a
single-purpose ephemeral toolchain — there is no system Python to protect,
and Debian's pip otherwise refuses even `--user`/`--target` installs.
Installing into the workspace persists across environments:
`pip install --user pkg==ver` lands in `/workspace/.local` (HOME is
/workspace) and is importable by later jobs with no extra flags; console
scripts land in `/workspace/.local/bin`, which egress-enabled ops put on
PATH (including login shells via `/etc/profile.d/sumi-egress-path.sh`);
a pinned `pip install --target` works the same way. Tag it with the full
40-hex revision of the source that produced it; the provisioner refuses a
tag that is not a full revision and verifies the image ID before launch.

```sh
docker build -t ghcr.io/sumi-studio/sumi-job:<40-hex-revision> -f deploy/job/Dockerfile .
```

## API configuration

All opt-in; partial configuration is a startup error, never a silently
queued backend.

| Variable | Default | Purpose |
| --- | --- | --- |
| `SUMI_JOBEXEC_ENABLED` | unset | `1/true/yes/on` starts the driver. |
| `SUMI_JOBEXEC_RUNNER_ID` | `jobexec-docker` | Stable claim identity; exactly one driver may run per id. |
| `SUMI_JOBEXEC_BACKEND` | `cloud` | Claim predicate this driver applies — keep `cloud`. |
| `SUMI_JOBS_DEFAULT_BACKEND` | `cloud` (when driver enabled) | Stamps `request.backend` on submissions that omit it. |
| `SUMI_JOBEXEC_LEASE_SECONDS` | `120` | Claim TTL; heartbeats renew every sweep. |
| `SUMI_JOBEXEC_INTERVAL_SECONDS` | `2` | Sweep interval. |
| `SUMI_JOBEXEC_CLAIM_LIMIT` | `2` | New jobs claimed per persona per sweep. |
| `SUMI_JOBEXEC_BACKEND_WAIT_SECONDS` | `600` | Bound on a definite capacity refusal (`busy`) before honest failure. |
| `SUMI_JOBEXEC_UNKNOWN_WAIT_SECONDS` | `600` | Bound on an indeterminate backend wait before the claim is released to the `lost` sweep — never a `failed` verdict. |
| `SUMI_JOBEXEC_OUTPUT_WAIT_SECONDS` | `120` | Bound on waiting for terminal output to become readable before completing with `*_unavailable` markers. |

Boot readiness: when the driver is enabled the API performs one bounded
`ProcessStatus` probe against the provisioner socket and **fails startup**
if the service cannot answer — a driver that cannot reach its backend must
never advertise capacity it does not have.

Required when enabled: the core state service, `SUMI_RUNTIME_PROVISIONER_SOCKET`,
and the canonical file service (`SUMI_FILESVC_URL` + token) — every launch
binds a verified files scope, so without filesvc there is no verified
scope-creation path and the driver refuses to start.

## Provisioner configuration

The root provisioner additionally reads:

| Variable | Purpose |
| --- | --- |
| `SUMI_JOB_IMAGE_TAG` | Full 40-hex image revision for `image:"job"` launches. |
| `SUMI_JOB_EGRESS_DIR` | Host directory holding the egress proxy unix socket (`/run/sumi/egress` in the reference compose). Set → job containers get the socket mount + loopback proxy env; empty/unset → jobs keep the exact no-network contract. Requires a job image containing `sumi-egress-bridge` and the `job-egress-proxy` service (or an equivalent `sumi-egress-proxy` process) serving `<dir>/proxy.sock`. |
| `SUMI_FILES_MOUNTPOINT` | Canonical files mount root on the provisioner host. |
| `SUMI_FILES_VOLUME_UUID` | Expected JuiceFS volume UUID. |
| `SUMI_FILES_CHECK` | Path to `deploy/files/sumi-files-check`. |
| `SUMI_FILES_CHECK_WAIT_SECONDS` | `--wait` bound for the mount check (default 15). |

When `SUMI_FILES_*` is incomplete, `files-scope` launches refuse with
`workspace_unavailable` — no fallback to the legacy named volume exists
for job operations. Legacy `workspace:""` process callers keep their
established named-volume contract.

## Placement and routing

Routing is deterministic in SQL, by `request.backend`:

- the Cloud driver claims with `backend="cloud"` — only requests stamped
  `backend:"cloud"`;
- the Local Node `JobRunner` sends no `backend` — its claim predicate is
  `COALESCE(request.backend,'local')='local'`, so it takes only
  unstamped/explicit-local work and **cannot** race Cloud jobs;
- `"*"` is an unrestricted claim for reconcilers/tests only — do not run a
  production runner with it on a routed deployment.

Deployment checklist for Cloud execution:

1. Run exactly one API instance with `SUMI_JOBEXEC_ENABLED=1`,
   `SUMI_JOBEXEC_BACKEND=cloud`, `SUMI_JOBS_DEFAULT_BACKEND=cloud`, a
   unique `SUMI_JOBEXEC_RUNNER_ID`, the provisioner socket, and filesvc.
   Boot fails if the provisioner cannot answer — fix the dependency, do
   not run the API half-wired.
2. Do not configure a second driver with the same runner id, and do not
   run a Local `JobRunner` extra host with a wildcard or `cloud` backend —
   it defaults to the local predicate and needs no change.
3. Personas intended for Local execution keep working: submit with
   `backend:"local"` (or leave unset with `SUMI_JOBS_DEFAULT_BACKEND=local`)
   and run the extra host for them as before.

Recovery verification: `ClaimJobs` applies the predicate inside the claim
transaction, and `ClaimJobsNeedingAttention`/`RunnableJobs` discover work
at job granularity with kind/owner/resolution filters — schema/query
behavior is what fences runners, not process placement.

## Recovery contract

- Runner identity is the configured `runner_id`, stable across restarts.
- Every sweep reconciles claimed work (heartbeat/cancel/complete, orphan
  stop + observed outcome on `lost`) **before** offering new claims.
- Unresolved work is discovered at job granularity — kind, claim owner and
  resolution filtered in SQL with a fair `(persona_id, job_id)` cursor —
  so an old live claim is never hidden behind newer resolved history.
- One job ↔ one deterministic operation ID (`persona × "job:"+job_id`); a
  possibly-launched operation is never re-created — absence in the durable
  journal is the only signal that may launch, and terminal
  `never_started`/`observed_absent` verdicts are gated on what the durable
  producer-side fence *returns*: `CancelProcess(TombstoneIfAbsent)` and
  `StartProcess` journal publication serialize under the provisioner's
  process-store mutex. Only a **tombstone** return proves the operation
  never journaled — a successful cancel alone is not proof, because a
  delayed start can journal between the driver's status read and the
  fence. A live or terminal operation returned by the fence keeps the job
  in reconcile and lands the honest outcome the real work produced.
- Ambiguous backend answers are never `failed`: a lost `StartProcess`
  response, status outage, or unreachable service records a durable
  `result.runner_wait` (cause + first-observed time — survives restarts),
  heartbeats continue until `SUMI_JOBEXEC_UNKNOWN_WAIT_SECONDS`, then the
  claim is released so the sweep commits `lost` and physical cleanup runs.
  Only typed refusals — invalid request, workspace refusal, divergent
  identity, or capacity `busy` past its bound — may complete `failed`.
- `lost` is final. The driver physically stops an orphaned container and
  records the real backend state under `result.observed_outcome` via
  `AttachLostOutcome` — the verdict never becomes a silent success, and
  nothing re-executes. Resolved lost rows leave the reconcile set.
- A cancelled verdict commits only after the backend reaches a durable
  terminal state — never while a process could still write, and
  `never_started` only when the fence *returned a tombstone* (the fence
  above) — a live or terminal operation it returns is reconciled to its
  honest outcome instead.
- Results carry truthful output: bounded sanitized content with explicit
  `*_truncated`/`*_unavailable` markers — never silent blanking.

## Lightweight script supervisor (`kind: "script"`)

The script backend is a separate long-running process, `apps/scripts`:

```
node apps/scripts/run.mjs            # Node >= 22.18 (types stripped on import)
cd apps/scripts && pnpm run run
```

Startup is all-or-nothing: a bad environment exits non-zero with the
named missing/invalid variable — the process never half-starts under an
identity or config it cannot sustain.

Required environment:

- `SUMI_STATE_API` — state API origin (`http://host:port`)
- `SUMI_STATE_TOKEN` — internal runtime credential (>=16 chars)
- `SUMI_WORKERD_BIN` — path to the workerd binary

Optional environment:

- `SUMI_CGROUP_MODE` — `systemd` | `prlimit` (default autodetect; an
  explicit `systemd` request fails honestly when scopes cannot launch —
  `prlimit` bounds CPU/wall/output and the V8 heap only)
- `SUMI_RUNNER_ID` — explicit durable identity; otherwise a random id is
  minted and persisted under `SUMI_WORK_DIR` (`runner-id` file). Claims
  never begin under an identity that cannot be recovered, so the
  attention route and lost-outcome attach stay reachable across restarts.
- `SUMI_WORK_DIR` — durable journal dir (default `~/.local/state/sumi-scripts`)
- `SUMI_PERSONAS` — comma-separated persona filter; **local/dev only**.
  Production discovery needs no persona list — the shared
  `GET /internal/core/jobs/runnable` route supplies it dynamically.
- `SUMI_LEASE_MS` (30000), `SUMI_HEARTBEAT_MS` (5000),
  `SUMI_CLAIM_LIMIT` (4), `SUMI_MAX_CONCURRENT` (4),
  `SUMI_POLL_MS` (1000), `SUMI_DISCOVERY_PAGE` (64),
  `SUMI_DISPATCHER_PATH`, `SUMI_RUNLIMITED_BIN`,
  `SUMI_SHUTDOWN_GRACE_MS` (20000), `SUMI_SHUTDOWN_SETTLE_MS` (3000)

### Shutdown contract (SIGTERM/SIGINT)

A real wall-clock bound of approximately `grace + settle + <1s` of local
kill work — not an open-ended drain:

1. Discovery and claims stop immediately. The runner's pre-spawn guard
   refuses new launches (spawn→register is synchronous, so no child can
   appear after the kill pass).
2. A claim request already in flight can resolve after the stop: those
   durable reservations are reported `cancelled` with reason
   `shutdown_before_start` — provably never executed — rather than
   stranded until lease expiry sweeps them to `lost`.
3. Every in-flight job gets `cancel_requested` **concurrently** (each
   call bounded by the client's own 15s request timeout, racing the
   drain — stalled API calls can only consume grace, never multiply
   it). Its drive loop observes the cancel at the next heartbeat and
   reports `cancelled` with whatever usage was measured.
4. At the grace deadline, surviving children are killed **locally** —
   each spawn is `detached`, so the wrapper leads an **owned process
   group** (`pgid == spawned pid`): `kill(-pgid)` is atomic across the
   fork→exec window where a payload child exists but answers to no
   `workerd` comm name — a descendant walk provably misses it there
   and killing only the wrapper would orphan it to init (observed as
   a real leak). Verified journal identities (boot_id + start_ticks)
   still kill a located payload first so `runlimited` can wait4 it
   and report measured usage; the group kill is the backstop. Never
   broad kills or guessed pids. Children are not left to "their own
   limits": `wall_ms` is enforced by this process's drive loop which
   dies with the process, and `RLIMIT_CPU` bounds CPU time — an idle
   or blocked child's elapsed lifetime is unbounded.
5. Up to `SUMI_SHUTDOWN_SETTLE_MS` more for the killed jobs' drive
   loops to unwind: `terminate` collects wait4 usage stats, `finish`
   attempts the honest report (`cancelled`/`failed` + measured or
   `unknown` usage). Past that window the supervisor returns anyway —
   journals hold the frozen wire payloads, undelivered claims expire
   to `lost`, and the next supervisor's startup reconcile delivers
   the evidence idempotently. `run.mjs` then exits explicitly —
   lingering HTTP/timer handles cannot extend the process past the
   bound.
6. A second signal is the emergency path: a synchronous local kill of
   every owned child, then immediate `exit(2)`. Whatever was journaled
   before the kill is the evidence; reconcile recovers the rest.
7. A fatal `start()` error after children launched runs the same
   bounded shutdown before exiting 1.

### Per-job fault containment

One job's preparation fault (mkdir/journal/config/spawn/pid-write) is a
contained `failed`/`runner_error` report for that job — never a process
exit that orphans siblings. If even the report fails (degraded storage
and API), the claim resolves honestly by lease expiry into the
attention path.

### Credential hygiene

`config.capnp` carries the runtime token only to launch a worker;
workerd reads it once at startup. The file is unlinked as soon as the
worker binds, and a `finally` over the whole launch domain removes it
on every other path too — ready failure, spawn/pid-persist fault,
partial write (the owned path is computed before `writeConfig` runs),
cancel and shutdown. Stale copies left by a dead run are swept before
the first claim at startup. Journals, rusage stats and run dirs are
kept — only the credential copy is removed. An unlink failure is
logged by path only (never contents); if the filesystem itself is
broken the residue is reported, not hidden.
