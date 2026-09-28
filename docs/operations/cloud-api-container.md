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
  returns. A write the API acknowledged is in PostgreSQL.
- On start, the API writes PostgreSQL's copy into the empty directories, then
  opens the journals as before. The existing journal code (flock, positional
  writes, rollback by truncation) is unchanged.
- Each starting API process takes a new owner epoch. A process whose epoch
  was superseded cannot commit journal writes (`ErrFenced`) and shuts itself
  down. Fencing prevents two hosts from diverging; it does not arbitrate
  availability, so the old host must be disabled at a move.
- If a host restarts with its disk intact, bytes after the mirrored copy
  (written but never acknowledged) are trimmed; any other difference stops
  startup with `ErrConflict` and changes nothing.

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
only add tables; existing rows are not rewritten. Version 0002 is left to
`0002_chatgpt_subscription` on the subscription branch; the migrator allows
the gap and applies these after it.

1. On the current WSL API, set `SUMI_API_JOURNAL_MIRROR=postgres` and
   restart it. The first start adopts the existing journal files into
   PostgreSQL (logged as `seeded from local files`); later writes are mirrored.
2. Maintenance window: stop the WSL API (and disable its restart policy),
   `pg_dump` the Sumi database, restore it into the hosted PostgreSQL.
   Alternatively point the WSL API at the hosted database first; then the
   container's start fences the WSL API instead of relying on the stop alone.
3. Compare: `SUMI_DB_URL=<hosted> sumi-journal-mirror verify --commands <wsl
   command-log copy> --browser-events <wsl browser-events copy>` must print
   `"equal":true` for both.
4. Deploy/start the container Worker against the hosted database; switch the
   Web/Core/browser Workers' bindings from the VPC Service to the service
   binding.

The file service database, file objects and LiveKit move separately.

## Checks

- `go test ./internal/journalmirror/ ./internal/agentevents/ ./internal/messaging/ ./internal/db/ ./cmd/server/`
  with `SUMI_TEST_DB_URL`. `TestAttachmentBlobBackendsShareTheContract` runs
  the same assertions against the disk and PostgreSQL attachment stores.
- `cmd/server/cloud_replacement_proof_test.go` (`SUMI_CLOUD_PROOF=1`) runs a
  Direct Chat exchange against a running API, replaces its host with an
  operator command, and requires identical history, the original receipt for
  a retried command and the next sequence number for a new one.
