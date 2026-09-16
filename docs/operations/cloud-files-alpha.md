# Shared files on Cloud (alpha)

A secretary's files live in one canonical JuiceFS volume, not on the machine
that runs its Linux executor. The files API (`filesvc`) and every executor
host are separate clients of that same volume:

- While the executor is stopped, people and the secretary still read and
  write the files through the files API.
- When the executor starts, its `/workspace` is the same directory. It sees
  the API's changes at once; there is no copy to synchronize.
- A file written on Linux is readable and listable through the API right
  away (version 0 until the API writes it; an API-versioned file edited on
  Linux reports `external_change: true`).
- Remounting, restarting `filesvc` or restarting the executor host reconnects
  to the existing files.
- Nothing mounts or serves a namespace that cannot be verified: a missing
  mount, a different volume, or a mount with metadata caching is refused.

The API semantics (versions, reserved `.filesv-op-*` recovery names,
settlement) are those of `contracts/files-api.yaml`; this document covers
placement, lifecycle and access.

## Route

```text
person/secretary ─ (caller with a scope token) ─ filesvc :8780 (WSL origin, user unit)
                                                   │ FILESV_REQUIRE_MOUNT=1
                                                   ▼
                          service JuiceFS client  ~/.local/state/sumi-files/mnt
                                 │ metadata                │ blocks (S3 API, SigV4)
                                 ▼                         ▼
                 PostgreSQL: sumi_files_meta     sumi-fabric-obj Worker → BucketObject DO
                                 ▲                         ▲
                                 │                         │
            executor host: root JuiceFS client /var/lib/sumi-files/mnt
                                 │ bind <mnt>/<scope> only
                                 ▼
                      executor container /workspace (uid 10002)
```

- `filesvc` keeps its own version/event state in a second database
  (`sumi_files`). One `filesvc` database binds to one root path forever; do
  not change `FILESV_ROOT` after the first start.
- Every client squashes all users to `10002:10002` (the executor uid), so a
  file created through the API can be edited on Linux and vice versa.
- Every client mounts with zero metadata caching. For `filesvc` this is a
  correctness requirement (the contract's CAS gate); for executors it removes
  a staleness window. It costs a metadata round trip per lookup, so executor
  hosts must be close to the metadata database (see Operating bounds).
- The executor container receives one bind: its scope directory. It gets no
  metadata or object-storage credential and cannot reach other scopes. The
  host credential file is root-only.

## Components

| Path | Role |
| --- | --- |
| `apps/files/cmd/filesvc` | Scoped file API. |
| `apps/files/objstore` | `sumi-fabric-obj`: S3-compatible object store on a SQLite Durable Object. Verifies AWS SigV4 (header form). |
| `deploy/files/sumi-files-format` | Creates the volume once; refuses an already formatted database; prints the UUID. |
| `deploy/files/sumi-files-mount` | Runs one JuiceFS client with the required flags. Refuses a metadata database holding a different volume UUID, or an existing mount. |
| `deploy/files/sumi-files-check` | Passes only for a live `fuse.juicefs` mount of the pinned volume root with zero metadata cache; with a scope, prints the directory to bind. |
| `deploy/files/systemd/sumi-files-mount@.service` | Volume client unit (`service` instance on the origin as a user unit; `executor` instance on executor hosts as a system unit). |
| `deploy/files/systemd/sumi-filesvc.service` | `filesvc`, bound to the service client. |
| `deploy/files/systemd/sumi-files-executor@.service` | Starts one secretary's agent Compose project with `/workspace` bound to scope `%i`; stops with the volume client. |
| `deploy/files/compose.executor-workspace.yaml` | Compose override replacing the executor's local `workspace` volume with the scope bind. |
| `scripts/operations/files-cloud-probe.mjs` | Post-deploy probe: API round trip, refusals, and (with `--mount-dir`) cross-client visibility. |

`sumi-files-check` refusal codes: `not_mounted`, `wrong_fstype`,
`subdir_mount`, `unreadable_config`, `wrong_volume`, `cache_policy`,
`scope_missing`. A bare directory left by a stopped mount is `not_mounted`.

## Credentials

| Name | Where | Purpose |
| --- | --- | --- |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | `sumi-fabric-obj` Worker secrets; given once to `sumi-files-format` | Object-store SigV4 pair. JuiceFS stores it in the volume settings in the metadata database. |
| metadata DB role password (`META_PASSWORD`) | `~/.config/sumi-files/service.env` (origin), `/etc/sumi-files/executor.env` (executor hosts), 0600 | Anyone holding it can read the object-store pair from the volume settings. Host-only. |
| `filesvc` DB DSN | `~/.config/sumi-files/filesvc.env` (0600) | Version/event state. |
| `FILESV_TOKENS` | `filesvc.env` | `token:scope[,scope]` grants separated by `;` (quote the value). Restart `filesvc` to change. |

Generate secrets with `openssl rand -hex 32` into 0600 files. Do not put them
in `wrangler.jsonc`, command lines, logs or documents. `juicefs status`
prints the access key id; the SigV4 check makes that id alone useless.

## Preconditions (checked 2026-09-16)

- **R2 is not enabled** on the Sumi Cloudflare account:
  `wrangler r2 bucket list` answers `Please enable R2 through the Cloudflare
  Dashboard. [code: 10042]`. The object store is therefore the
  `sumi-fabric-obj` Durable Object Worker, which is **not yet deployed**
  (`wrangler deployments list --name sumi-fabric-obj` → code 10007).
- The Workers plan of the account was not established from the CLI. It
  decides whether block uploads fit the CPU limit (see Operating bounds).
- The JuiceFS client used for all local verification is v1.4.1
  (`juicefs version 1.4.1+2026-07-30.0b90c7d`). Pin the same version on every
  client.
- Executor hosts need FUSE (`/dev/fuse`), Docker, root for the volume client,
  a network path to the metadata PostgreSQL with TLS, and HTTPS to
  `workers.dev`.

## Apply

### 1. Object store Worker

From `apps/files/objstore` with Node 22 and the Wrangler login for the Sumi
account:

```sh
umask 077; d=~/.local/share/sumi-files-alpha; mkdir -p "$d"
for f in s3-access-key s3-secret-key; do [ -s "$d/$f" ] || openssl rand -hex 32 > "$d/$f"; done
node node_modules/wrangler/bin/wrangler.js deploy --dry-run    # expect BUCKET (BucketObject), no vars
node node_modules/wrangler/bin/wrangler.js deploy
node node_modules/wrangler/bin/wrangler.js secret put S3_ACCESS_KEY < "$d/s3-access-key"
node node_modules/wrangler/bin/wrangler.js secret put S3_SECRET_KEY < "$d/s3-secret-key"
curl -s "https://sumi-fabric-obj.<subdomain>.workers.dev/healthz"      # ok
curl -s -o /dev/null -w '%{http_code}\n' "https://sumi-fabric-obj.<subdomain>.workers.dev/sumi-files-alpha"  # 403
```

### 2. Databases

On the PostgreSQL server that the origin uses (or a dedicated one), create two
databases with their own roles: `sumi_files_meta` (JuiceFS metadata) and
`sumi_files` (`filesvc`). Executor hosts need network access only to
`sumi_files_meta`; use `sslmode=require` (or `verify-full`) off-host. Back up
both databases with the normal PostgreSQL backup; the mount units disable
JuiceFS's own metadata dump (`--backup-meta 0`).

### 3. Install on the origin (user units)

```sh
install -d ~/.local/lib/sumi-files ~/.config/sumi-files ~/.local/state/sumi-files/mnt ~/.cache/sumi-files
(cd apps/files && go build -buildvcs=false -o ~/.local/lib/sumi-files/filesvc ./cmd/filesvc)
install -m 0755 deploy/files/sumi-files-{format,mount,check} ~/.local/lib/sumi-files/
install -m 0755 <pinned juicefs 1.4.1 binary> ~/.local/lib/sumi-files/juicefs
install -m 0644 deploy/files/systemd/sumi-files-mount@.service deploy/files/systemd/sumi-filesvc.service ~/.config/systemd/user/
```

`~/.config/sumi-files/format.env` (0600, delete after formatting):

```sh
SUMI_FILES_JUICEFS=/home/<user>/.local/lib/sumi-files/juicefs
SUMI_FILES_META_URL=postgres://sumi_files_meta@<host>:5432/sumi_files_meta?sslmode=disable
META_PASSWORD=<meta role password>
SUMI_FILES_VOLUME_NAME=sumi-files-alpha
SUMI_FILES_BUCKET_URL=https://sumi-fabric-obj.<subdomain>.workers.dev/sumi-files-alpha
ACCESS_KEY=<s3-access-key>
SECRET_KEY=<s3-secret-key>
```

```sh
~/.local/lib/sumi-files/sumi-files-format ~/.config/sumi-files/format.env   # prints the volume UUID
```

`format` writes and reads a test object, so it also proves the Worker's
SigV4 pair. `~/.config/sumi-files/service.env` (0600):

```sh
SUMI_FILES_ROLE=service
SUMI_FILES_JUICEFS=/home/<user>/.local/lib/sumi-files/juicefs
SUMI_FILES_META_URL=postgres://sumi_files_meta@<host>:5432/sumi_files_meta?sslmode=disable
META_PASSWORD=<meta role password>
SUMI_FILES_VOLUME_UUID=<uuid printed by format>
SUMI_FILES_MOUNTPOINT=/home/<user>/.local/state/sumi-files/mnt
SUMI_FILES_CACHE_DIR=/home/<user>/.cache/sumi-files
SUMI_FILES_CACHE_SIZE_MIB=10240
SUMI_FILES_OWNER=10002:10002
```

`~/.config/sumi-files/filesvc.env` (0600):

```sh
FILESV_LISTEN=127.0.0.1:8780
FILESV_ROOT=/home/<user>/.local/state/sumi-files/mnt
FILESV_DB_URL="postgres://sumi_files:<password>@<host>:5432/sumi_files?sslmode=disable"
FILESV_TOKENS="<token>:<scope>;<probe-token>:<scope>"
FILESV_REQUIRE_MOUNT=1
```

```sh
systemctl --user daemon-reload
systemctl --user enable --now sumi-files-mount@service.service sumi-filesvc.service
systemctl --user status sumi-files-mount@service.service sumi-filesvc.service
```

The unit's `%E/sumi-files/%i.env` resolves to
`~/.config/sumi-files/service.env` for the `service` instance.

### 4. Provision a scope and probe

A scope directory exists once the API has created something in it; an
executor for a scope that does not exist yet is refused (`scope_missing`).

```sh
curl -sS -X POST -H "Authorization: Bearer $(cat <token-file>)" \
  --data '{"path":"notes"}' "http://127.0.0.1:8780/v1/files/<scope>/mkdir"
node scripts/operations/files-cloud-probe.mjs --api http://127.0.0.1:8780 \
  --token-file <probe-token-file> --scope <scope> --other-scope <a scope the token lacks>
```

### 5. Executor hosts (system units, root)

```sh
install -d /root/.local/lib/sumi-files /etc/sumi-files /var/lib/sumi-files/mnt /var/cache/sumi-files
install -m 0755 deploy/files/sumi-files-{mount,check} <juicefs 1.4.1> /root/.local/lib/sumi-files/
install -m 0644 deploy/files/systemd/sumi-files-mount@.service deploy/files/systemd/sumi-files-executor@.service /etc/systemd/system/
```

`/etc/sumi-files/executor.env` (0600, root): as `service.env` with
`SUMI_FILES_ROLE=executor`, `SUMI_FILES_JUICEFS=/root/.local/lib/sumi-files/juicefs`,
`SUMI_FILES_MOUNTPOINT=/var/lib/sumi-files/mnt`,
`SUMI_FILES_CACHE_DIR=/var/cache/sumi-files` and `sslmode=require`.
`/etc/sumi-files/agent-<scope>.env`: `SUMI_AGENT_COMPOSE_DIR` (a checkout
holding `deploy/agent` and `deploy/files`) and the agent Compose variables.

```sh
systemctl daemon-reload
systemctl enable --now sumi-files-mount@executor.service
systemctl enable --now sumi-files-executor@<scope>.service
node scripts/operations/files-cloud-probe.mjs --api <filesvc URL reachable from this host> \
  --token-file <probe-token-file> --scope <scope> --mount-dir /var/lib/sumi-files/mnt/<scope>
```

The root volume client adds `allow_other`, which Docker and uid 10002 need.
Running the probe with `--mount-dir` from the executor host is the check that
an API write and a Linux write cross between two JuiceFS clients.

## Verify with real use

1. With the executor stopped (`systemctl stop sumi-files-executor@<scope>`
   and `sumi-files-mount@executor`), write and rename files through the API.
   Start both; inside the executor, `cat` and `ls` show exactly those files.
2. Inside the executor, write a file under `/workspace`; read and list it
   through the API with no other step.
3. `systemctl restart sumi-files-mount@executor.service`: the executor unit
   restarts with it and sees the same files. `kill -9` the client: the
   running executor gets `Transport endpoint is not connected`, not old
   files; start the executor unit again once the client is back. Restart `sumi-filesvc` and read a
   file: `X-File-Version` is unchanged.
4. `systemctl --user stop sumi-files-mount@service` stops `filesvc` too; if
   the mount disappears while `filesvc` runs, API calls answer 503
   `mount_unavailable` and nothing is written into the bare directory.
5. Watch `wrangler tail` on `sumi-fabric-obj` during a large write: no
   `exceededCpu`/1102 errors, no 403s.

## Rollback

- Executors: `systemctl disable --now sumi-files-executor@<scope>` and start
  the agent project without `compose.executor-workspace.yaml` (host-local
  `workspace` volume). Files already in the volume stay there.
- Files API: `systemctl --user disable --now sumi-filesvc sumi-files-mount@service`.
  Callers get connection refused, not wrong files.
- The Worker, the volume data (DO storage) and both databases are kept.
  Deleting the `sumi-fabric-obj` Worker deletes its Durable Object data; do
  that only when the volume is intentionally discarded, together with
  `sumi_files_meta` and `sumi_files`.
- Re-enable by starting the units again with the same UUID; nothing is
  re-formatted.

## Operating bounds

Primary sources, read 2026-09-16:
<https://developers.cloudflare.com/durable-objects/platform/limits/>,
<https://developers.cloudflare.com/durable-objects/platform/pricing/>,
<https://developers.cloudflare.com/workers/platform/limits/>,
<https://developers.cloudflare.com/r2/pricing/>.

- **One Durable Object holds the whole bucket.** SQLite storage per object is
  10 GB on Workers Paid (1 GB per object and 5 GB account total on Free), so
  the alpha volume's block data is capped there. Rows are 512 KiB chunks,
  under the 2 MB row limit. One object also serializes all block requests
  (soft limit 1,000 requests/s).
- **CPU per request.** A block upload is read, SHA-256 hashed for SigV4 and
  written in the Worker/DO. Workers Free allows 10 ms CPU per request; a
  4 MiB block will likely exceed that. Paid allows 30 s by default. Confirm
  the account plan before relying on uploads, and watch for CPU errors.
- **Quotas and cost.** Free: 100,000 Worker requests/day, 100,000 DO requests/
  day, 100,000 rows written/day, 5 GB stored. Paid: 1 M DO requests/month
  then $0.15/M; 50 M rows written/month then $1.00/M; 5 GB-month then
  $0.20/GB-month. A 4 MiB block is one request and 9 row writes (8 chunks +
  metadata row), so 1 GiB of new data ≈ 256 requests and ≈ 2,300 rows.
  Reads of blocks not in a client's local cache are billed as requests and
  rows read.
- **R2 alternative.** R2 would remove the single-object cap (free tier
  10 GB-month, 1 M Class A / 10 M Class B operations per month, no egress
  fee). It must be enabled on the account first. Moving an existing volume to
  R2 means copying its objects and changing the volume's bucket
  (`juicefs config --bucket`), so decide before real data accumulates.
- **Metadata latency.** Zero metadata caching sends every lookup to
  PostgreSQL. JuiceFS warns above a few milliseconds of latency. Executor
  hosts far from the database will make ordinary tools (git, package
  managers) slow; place them near the database.
- **Object store outage** (measured against local workerd, 2026-09-16):
  - API write: 503 `unavailable` after ~30 s; never 200. The JuiceFS client
    keeps retrying the upload and gives up after 5 minutes
    (`flush … timeout after waited 5m0s`). Retrying the write after storage
    returns can answer 503 `pending_settlement` until the reconciler settles
    the interrupted write (25–95 s after restore in two runs), then succeeds.
    The interrupted write's staged bytes surfaced as a public
    `recovered-o<id>-<random>` file next to the target, as the contract
    specifies for unattributable staged objects; callers or people may need
    to clean these up.
  - API read of bytes not in the service client's block cache: 503
    `content_unavailable` (the first 256 KiB are read before the status is
    sent). Before this change the service answered 200 and closed the
    connection with no body. A failure later in a large file still shows
    only as a body shorter than `Content-Length`.
  - API stat/list and reads of cached blocks keep working (metadata is in
    PostgreSQL).
  - Executor write: `cp` + `sync` neither succeeded nor failed during a
    90–270 s outage; it completed once storage returned and the API read the
    same bytes. After 5 minutes JuiceFS reports the flush as failed.
- **Mount restart and running executors.** A running executor holds a bind to
  the client that existed when it started. If that client is killed, the
  bind answers `Transport endpoint is not connected` and keeps doing so after
  a new client mounts; it never falls back to a stale copy. After SIGTERM
  with a bind still held, the client unmounts the host mountpoint but keeps
  serving that bind until it is killed. `sumi-files-executor@` is `BindsTo`
  and `PartOf` the volume client: a restart of the client restarts the
  executor; after a client crash the executor is stopped and must be started
  again (`systemctl start sumi-files-executor@<scope>`).
- **`FILESV_TOKENS` is static.** Grants change only by editing the env file
  and restarting `filesvc`. Issuing per-secretary scope tokens from account
  authorization, and routing Cloud callers to `filesvc`, are separate
  integrations.

## What has and has not been verified

Verified locally on 2026-09-16 (details in the release handback): one WSL
kernel, PostgreSQL 17 in Docker, the object-store Worker in **local workerd**
(`wrangler dev`), a service JuiceFS client plus `filesvc` on the host, and an
executor-host stand-in container with its own root JuiceFS client and
executors as uid 10002 in a private mount namespace holding only the scope
bind. All mounts went through `sumi-files-mount`/`sumi-files-check`. Checks:
API read/write/rename with the executor host stopped; the executor sees
those files on start; API overwrites visible to the executor immediately;
Linux writes readable and listable through the API; direct edits reported as
`external_change` with CAS refused; token and executor scope isolation;
refusal for missing mount, other volume, cached mount and unprovisioned
scope; `filesvc` 503 without its mount and recovery on remount; `filesvc`
restart; full remount persistence; object-store outage and recovery; SigV4
positive/negative cases; `files-cloud-probe.mjs --mount-dir` from the
executor host.

Not verified: the deployed Worker and Durable Object (TLS, real edge header
handling, CPU limits, whether a remote client signs `UNSIGNED-PAYLOAD`), R2,
a separate executor host over the network, Docker executor containers from
the agent image using the Compose override (the merged Compose file was
rendered, not run; the agent `prepare` step's `install -d -o 10002` on the
bound scope is untested), and `systemd` unit operation (the unit files pass
`systemd-analyze verify` only).
