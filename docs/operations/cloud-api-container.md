# Go API in a Cloudflare Container (first WSL-independent slice)

This moves the Go API process off the WSL host. It is a source slice with
local-runtime proof, not a deployed or accepted production path.

```text
browser ─ sumi-proof (Web Worker) ── service binding ──┐
Core DO (sumi-core-proof) ── SUMI_STATE service binding ─┤
                                                         ▼
                        sumi-api-proof Worker → SumiApiContainer (one instance "origin")
                                                         │ Go API, unchanged routes
                                                         ▼
                                 hosted PostgreSQL (direct, session-mode connection)
```

The Web and Core Workers keep their code; only the binding changes from the
VPC Service (tunnel to WSL) to a service binding to the `api-container`
Worker (`apps/api-container`).

## What survives what

| State | Where it lives | Survives a container stop/replacement |
| --- | --- | --- |
| Accounts, workspaces, Messaging, Core state, approvals, model connections | PostgreSQL | yes |
| Direct Chat command log, browser event log, logged-out browser sessions (`SUMI_COMMAND_LOG_DIR`, `SUMI_BROWSER_EVENT_DIR`) | local files, mirrored to PostgreSQL (`api_journal_mirror_*`) | yes: restored before the API serves |
| Messaging attachment bytes | PostgreSQL (`messaging_attachment_blobs`, `SUMI_MESSAGING_ATTACHMENT_STORE=postgres`) when the attachment caps are set; otherwise attachments are disabled and uploads answer 503 | yes |
| WebSocket connections, in-process caches | process memory | no: browsers reconnect and resume from their last sequence |

A Cloudflare Container's disk is always discarded when the instance stops
(https://developers.cloudflare.com/containers/platform-details/architecture/).
The container therefore runs with `SUMI_API_JOURNAL_MIRROR=postgres`
(set by `deploy/api-container/entrypoint.sh`):

- Every journal `fsync` also commits the written bytes to PostgreSQL before it
  returns. A write the API acknowledged is in PostgreSQL. If the commit fails
  or its outcome is unknown (the connection broke after `COMMIT`), the write
  is reported as failed and the journal rolls it back locally as it does for
  a disk error; the next write first makes PostgreSQL's copy equal to the
  local file again (only the uncertain tail is sent). A database outage
  therefore fails the writes made during it and nothing else: Direct Chat
  is not disabled, and it works again when the database answers.
- On start, the API writes PostgreSQL's copy into the directories, then
  opens the journals as before. The existing journal code (flock, positional
  writes, rollback by truncation) is unchanged.
- The mirror must have been **initialized explicitly** (see below). The
  container's mode `postgres` is restore-only: a database without an
  initialized mirror stops the start with `ErrNotInitialized`; it is never
  taken as empty journals.

### Initialization, restart and refusals

| Situation | What happens |
| --- | --- |
| New installation, no journals anywhere | Run `sumi-journal-mirror init-empty` once against the database. It refuses a database that already has accounts or agents. |
| The current host holds the journals | Start it once with `SUMI_API_JOURNAL_MIRROR=postgres-adopt`. It copies its journal files into PostgreSQL as the first copy (logged `initialized from local files`) and then behaves like `postgres`. An empty directory is never adopted. |
| Container starts (empty disk) | PostgreSQL's copy is restored. |
| Host restarts with its disk (crash, rollout) | Files equal to PostgreSQL's copy are kept. A file that differs (a write that was never acknowledged, or a rollback whose own commit failed) is **moved** to `<dir>/.journal-mirror-quarantine/<time>-epoch<N>/` and PostgreSQL's copy is written in its place. No manual step. |
| The directory belongs to another mirror, or the database is older than what this host acknowledged (an older backup), or unmarked files differ from the mirror (e.g. an empty mirror was initialized before the host that holds the journals adopted them) | The start stops with `ErrWrongMirror` or `ErrMirrorBehind` and **changes nothing** in either copy. Attach the right database; nothing has to be repaired. |

Each directory carries a marker (`.journal-mirror`, the mirror's lineage)
and, per file, the generation of its last acknowledged change
(`.<file>.acked`); these distinguish an ordinary unacknowledged write from a
wrong database. Quarantined files are kept until an operator removes them;
they are never read by the API.

### One owner at a time (lease)

A starting API takes the mirror lease: a PostgreSQL session advisory lock on
a dedicated connection. While another process holds it, the start **waits**
(logging the holder; `SUMI_API_JOURNAL_MIRROR_ACQUIRE_TIMEOUT`, default no
limit) and answers 503 `api_starting` / `journal_lease`. It never takes the
journals from a running owner.

The owner watches its lease session. When the session ends (database
restart or failover, a broken connection, `pg_terminate_backend`), the loss
is noticed at once; a connection that silently stops answering is noticed
within 10 s (a 5 s check interval plus a 5 s read-only check). The API then
answers 503 `api_stopping`, stops its background work and browser
connections, and exits; the platform starts it again. Every journal write
also re-checks the owner epoch in its own transaction, so a process that
lost the lease cannot acknowledge a journal write even before it notices.
The database releases the lease of a host that vanished without closing
its connection after TCP keepalives fail (about 30 s).

This protects the journals. It is **not** a fence for everything the
process does: between the end of its lease session and its exit (normally
milliseconds, at most the 10 s window above) an old process may still finish
an in-flight request or background step. Most background loops claim their
work in PostgreSQL and cannot act twice; the exceptions, which can repeat
an effect when two API processes overlap, are:

- job execution (`SUMI_JOBEXEC_*`): the runner id defaults to the same value
  on every host, so a second host can relaunch a running job;
- email delivery: a claim lease of 60 s with sends of up to 30 s can send
  one email twice;
- Core wakes and cloud-browser wakes: duplicate wakes only (harmless).

An API started without the mirror (the current WSL host without
`SUMI_API_JOURNAL_MIRROR`) takes no lease at all. A move therefore always
stops the old host **and disables its restart** first.

### Start-up time and network budget

The API listens before it builds the application and answers 503 with
`Retry-After` and the phase (`database`, `journal_lease`,
`journal_restore` with file and byte progress, `opening`) until it is ready.
Restoring streams one 64 KiB chunk at a time (memory does not grow with the
journals) and is bounded by `SUMI_API_JOURNAL_MIRROR_RESTORE_TIMEOUT`
(default 30m); files restored before an interruption are kept, so the next
start continues.

Each acknowledged journal write costs one round trip to PostgreSQL and
sends about the written bytes plus ~500 bytes: a Direct Chat command is one
round trip, a projected browser-event batch two (dedup index, then events).
Measured on one machine with 20 ms injected per flight: 1.1 round trips and
984 bytes per 500-byte append; a restore at 12.5 MiB/s through a 5 ms,
16 MiB/s link. These are local measurements, not Cloudflare measurements:
place the database in the container's region so the round trip stays in the
low tens of milliseconds, and measure it before the move. Production
journals were about 30 KB in total on 2026-09-28; they only grow, and
retention is a separate product decision (history is never compacted by
this mirror).

Attachment bytes use the same `messaging.AttachmentBlobs` contract as the
disk store (staging, no-replace publish, sweep, reconciliation). The
entrypoint selects the PostgreSQL store whenever any
`SUMI_MESSAGING_ATTACHMENT_*_QUOTA_*` cap is set; all four caps are then
required, as on WSL. Bytes are kept in 256 KiB chunks and downloads read one
chunk at a time. Each attachment is at most 20 MiB and the whole store is
bounded by the total caps, so the database size stays within what the
operator configures. Existing attachments on a disk store would have to be
copied into the table before a move; the current production host has none.

## Idle policy and cost

The API runs background work that no request triggers: waking secretaries
with queued input or due schedules, Messaging/feedback attention delivery,
email delivery and transfer/return expiry. The default idle policy is
`keep`: the instance is not stopped when requests stop, and a once-a-minute
cron trigger starts it again after a crash or rollout. `sleep` stops it after
15 minutes without requests and is only correct once that background work is
driven from outside the process (for example, from Core Durable Object
alarms).

Measured on the current WSL API: about 20 MiB resident and about 1% of one
core. Instance type `basic` (1/4 vCPU, 1 GiB, 4 GB disk) leaves room; `lite`
(1/16 vCPU, 256 MiB) may suffice but has not been measured under load.
With Cloudflare's published rates
(https://developers.cloudflare.com/containers/pricing/), an always-running
`basic` instance is roughly: memory (730 − 25 included) GiB-h ≈ US$6.3/month,
disk ≈ US$0.7/month, CPU small at ~1% use — plus the US$5 Workers Paid plan.
Confirm whether vCPU is billed on active or provisioned time for the account
before relying on the CPU figure.

## Account and product prerequisites (not provisioned by this slice)

- **Workers Paid** on the Sumi account; Containers are not available on the
  Free plan.
- **A hosted PostgreSQL 17** reachable over TLS from the internet, accepting
  direct *session-mode* connections. The API holds session-level advisory
  locks (migrations, terminal runner lock) and long-lived lock connections,
  so a transaction-mode pooler (PgBouncer transaction mode, Hyperdrive) is
  not suitable, and Hyperdrive is not needed for a container. Candidates
  include PlanetScale Postgres, Neon, Supabase, Crunchy Bridge or RDS; choose
  a region near the users (Japan). A PostgreSQL inside a Container is not an
  option: its disk does not persist.
- **A Google credential for Firebase Admin** usable outside the WSL host
  (service-account key JSON passed as `SUMI_GOOGLE_CREDENTIALS_JSON`), with the
  roles the current API credential has.
- **Firebase authorized domain** for the proof Web Worker's hostname.

## Still on WSL after this slice

| API dependency | Today | Next owner |
| --- | --- | --- |
| Terminal and job runners (`SUMI_RUNTIME_PROVISIONER_SOCKET`, Docker) | WSL Docker via a Unix socket | terminal lane: a network runner contract; leave `SUMI_TERMEXEC_ENABLED`/`SUMI_JOBEXEC_ENABLED` unset in the container (they fail startup without the socket) |
| Files (`SUMI_FILESVC_URL`) | `sumi-filesvc.service` + JuiceFS on WSL | files lane; transitional option: a Container outbound handler for a private hostname that forwards through a Workers VPC Service |
| Calls (`SUMI_LIVEKIT_*`) | LiveKit container on WSL | needs LiveKit Cloud (external account) or a Cloudflare Realtime design; a Container cannot accept media UDP |

## Deploy the isolated proof

From the repository root, with the Sumi Wrangler login:

```sh
cd apps/api-container
npx wrangler deploy --env proof            # builds and pushes the image
for name in SUMI_DB_URL SUMI_CORE_STATE_TOKEN SUMI_CORE_RUNTIME_TOKEN SUMI_CORE_WAKE_TOKEN \
  SUMI_CORE_WAKE_URL SUMI_BROWSER_SESSION_SECRET SUMI_BROWSER_SESSION_AUDIENCE \
  SUMI_BROWSER_WS_ALLOWED_ORIGINS SUMI_MODEL_CONNECTION_KEY SUMI_AUTH_FIREBASE_PROJECT_ID \
  SUMI_AUTH_TENANT_ID SUMI_GOOGLE_CREDENTIALS_JSON; do
  npx wrangler secret put "$name" --env proof     # value from the operator's secret files
done
cd ../core && npx wrangler deploy --env proof   # plus its runtime/wake secrets
cd ../web  && npx wrangler deploy --env proof   # after building dist
```

Every `SUMI_*` Worker secret or variable is forwarded to the container except
the container-local ones (`SUMI_API_JOURNAL_MIRROR`, `SUMI_COMMAND_LOG_DIR`,
`SUMI_BROWSER_EVENT_DIR`, `SUMI_RUNTIME_PROVISIONER_SOCKET`,
`SUMI_MESSAGING_ATTACHMENT_ROOT`, `SUMI_MESSAGING_ATTACHMENT_STORE`, `PORT`).

## Moving the current host's state

No data is regenerated or imported; the same rows and journal bytes move.
The new migrations (`0003_api_journal_mirror`, `0004_messaging_attachment_blobs`)
only add tables; existing rows are not rewritten. Version 0002 is
`0002_chatgpt_subscription` from the subscription branch; the production
build must contain 0001–0004 in order. Never apply a build that lacks 0002
(0001, 0003, 0004 only) to a database that will be kept: the migrator
refuses the later build's history instead of guessing (see below).

1. Take a backup of the current database and copy both journal directories
   aside (read-only copies for `verify`).
2. On the current WSL API, set `SUMI_API_JOURNAL_MIRROR=postgres-adopt` and
   restart it. The first start copies its journal files into PostgreSQL
   (logged `initialized from local files`, with file and byte counts). From
   then on every acknowledged write is mirrored. Leave it in this mode.
3. Maintenance window: stop the WSL API **and disable its restart**
   (systemd unit / supervisor), so nothing can write the old journals again.
4. `pg_dump` the Sumi database and restore it into the hosted PostgreSQL.
   (Alternatively the WSL API was already pointed at the hosted database in
   step 2; then no dump is needed and the lease keeps a second process out.)
5. Compare the stopped host's directories with the hosted database:
   `SUMI_DB_URL=<hosted> sumi-journal-mirror verify --commands <command-log>
   --browser-events <browser-events>` must print `"equal":true` for both.
6. Deploy/start the container Worker against the hosted database
   (`SUMI_API_JOURNAL_MIRROR=postgres`, restore-only). Its log shows the
   restore with the same file and byte counts; its 503 answers name the phase
   until it is ready. Then switch the Web/Core/browser Workers' bindings from
   the VPC Service to the service binding.

If step 6 refuses (`ErrNotInitialized`, `ErrWrongMirror`,
`ErrMirrorBehind`), nothing was changed: check that the container uses the
database from step 4. Do not run `init-empty` on a database that has
accounts — that is what would make an empty journal authoritative; the
command refuses it for that reason.

A fresh installation without users skips steps 1–5 and runs
`SUMI_DB_URL=<db> sumi-journal-mirror init-empty` once before the first
container start.

### Schema history mismatch

If the API stops with `schema history mismatch`, the database was migrated
by a different build (a later one, another branch's order, or an edited
migration). Nothing was changed. Stop, and run the build whose migration
history matches the database, or release the missing migration in order.
Never drop or recreate a database that holds user data to get past it.

The file service database, file objects and LiveKit move separately.

## Checks

- `go test ./internal/journalmirror/ ./internal/agentevents/ ./internal/messaging/ ./internal/db/ ./cmd/server/`
  with `SUMI_TEST_DB_URL`. `TestAttachmentBlobBackendsShareTheContract` runs
  the same assertions against the disk and PostgreSQL attachment stores.
  The journal mirror's failure and restore contracts are in
  `internal/journalmirror/{mirror,lease,perf}_test.go`,
  `internal/agentevents/journal_mirror_test.go` and
  `cmd/server/journal_mirror_startup_test.go`.
- `cmd/server/cloud_replacement_proof_test.go` (`SUMI_CLOUD_PROOF=1`) runs a
  Direct Chat exchange against a running API, replaces its host with an
  operator command, and requires identical history, the original receipt for
  a retried command and the next sequence number for a new one.
