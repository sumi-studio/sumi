# Local host — run Sumi without a Cloud account

`deploy/local-host/sumi-local` installs and runs the new Sumi core on one
machine. It needs no Sumi Cloud account and no global system services.
One command owns two processes:

- **state service** — `apps/api/cmd/first-model`: the persona-scoped Go
  service holding canonical state in PostgreSQL, plus a minimal scoped
  browser/chat surface.
- **secretary** — `apps/core` local host (`node src/host/local.ts`, run by
  Node's native TypeScript support): the continuing secretary process that
  acquires the writer lease, works the durable input queue, calls the
  model, and recovers interrupted work.

Because PostgreSQL is the canonical store, the same secretary — identity,
history, queued work, schedules — persists across `stop`/`start`, ordinary
crashes, and reinstalls.

## Requirements

- Linux (the supported target; see [ADR 0004](adr/0004-agent-local-platform-support.md)).
  Process identity is verified through `/proc` (recorded start-time +
  absolute-path cmdline match before any signal), so lifecycle commands
  require Linux and fail clearly elsewhere. Native Windows is unsupported;
  use WSL.
- bash ≥ 5, `curl`, `openssl`, `flock`, `tar`, `nohup`, `realpath`
- Node ≥ 22.18 or ≥ 23.6 (runs `.ts` directly; `sumi-local doctor` checks)
- PostgreSQL, either:
  - `--db-url postgres://…` to a database you provide, or
  - `--managed-pg` (default when Docker is available): a dedicated
    loopback-only `postgres:17-alpine` container + named volume per
    `deploy/local-host/compose.pg.yaml`, scoped per install (see below).
    Not a shared system service — `restart: "no"`, started/stopped by
    `sumi-local`.
- Go toolchain only when installing from a source checkout. Packs carry a
  prebuilt binary; installing a pack does not need Go.

## Quick start

```sh
deploy/local-host/sumi-local install --db-url postgres://…   # or --managed-pg
sumi-local start
sumi-local status
sumi-local url          # browser URL (carries the fm credential)
sumi-local say --wait 60 "hello"
sumi-local logs         # service + secretary logs
sumi-local stop
sumi-local uninstall            # removes executables, keeps your data
sumi-local uninstall --purge    # also deletes the state home (+ managed PG data)
```

Installing drops a `sumi-local` shim in `~/.local/bin` once the install
completes. A failed install (for example, no database configured) leaves
an existing shim pointing at the previous install and its half-written
payload removable by `uninstall`.

## Layout

| Path | Contents | Survives `uninstall`? |
| --- | --- | --- |
| `~/.local/lib/sumi-local` (`--prefix`) | executables: service binary, `core/` TypeScript sources, CLI, `.sumi-local-prefix` ownership marker | no — payload entries removed; foreign files kept |
| `$XDG_STATE_HOME/sumi/local` (`--home`) | `config.env` (0600: identity, secrets, model), `.sumi-local-home` ownership marker, `run/` pids, `log/`, `workspace/` | yes — `--purge` to delete |
| `~/.local/bin/sumi-local` | CLI symlink — a regular file there is never overwritten (`install` refuses) | no — removed |
| managed PG volume | canonical database (managed mode) | yes — `--purge` deletes it, after an ownership check |

Identity lives in `config.env`: `SUMI_PERSONA_ID` is generated once at
first install and preserved on reinstall — that is what makes the
reinstalled secretary *the same individual* with the same history.
`.sumi-local-home` is the ownership marker `install` writes next to it:
it records `SUMI_LOCAL_ID` *and* the install prefix, so `uninstall`/
`--purge` can still prove the home is a sumi-local install, find its
payload root, and name its managed docker resources even after
`config.env` is deleted or corrupted. `.sumi-local-prefix` in the install
prefix records the same id, which is what links a home/prefix pair: a
prefix marked for a different install is refused, and a config whose id
disagrees with the home marker refuses destructive action rather than
picking a side. Generic directories (`run/`, `log/`, `workspace/`) are
*not* ownership evidence, and `config.env` itself is parsed as data —
its contents are never executed as shell. The config is a flat
`KEY=value` file (LF or CRLF endings; if a key appears twice the later
line wins); anything that doesn't match is ignored, so a foreign or
corrupt file is inert rather than half-loaded.

`config.env` parsed as data also means the process-anchored safety nets
need one more proof: a reused *path* is not ownership. Because a state
home survives `uninstall` and a later install may claim the same prefix
directory, every prefix-anchored `/proc` sweep (`stop`, `start`,
`restart`, `uninstall`) first checks that `$PREFIX/.sumi-local-prefix`
still names *this* install's id. A prefix whose marker is absent,
unparseable, or names another install is left alone — its processes are
reported, never signaled, and its payload entries are never deleted.

## Multiple installs / non-default paths

Each install is identified by `SUMI_LOCAL_ID` (recorded in `config.env`,
derived once from the state-home path). Managed docker resources are named
after it — container `sumi-local-pg-<id>`, volume `sumi-local-pgdata-<id>`,
compose project `sumi-local-<id>` — so two installs never share data, and
a second install's `--purge` cannot delete the first's volume. The managed
PG host port is docker-assigned (`127.0.0.1:0`), so two managed installs
can even run simultaneously (each still needs its own
`SUMI_LOCAL_LISTEN`).

With non-default `--home`/`--prefix`, later commands select the install
via the environment — `install` prints the exact lines:

```sh
export SUMI_LOCAL_HOME=/path/to/state-home
export SUMI_LOCAL_PREFIX=/path/to/prefix
sumi-local start
```

The two must name the *same* install: `config.env` records
`SUMI_LOCAL_INSTALLED_PREFIX`, and lifecycle commands refuse a
`SUMI_LOCAL_HOME`/`SUMI_LOCAL_PREFIX` pair that points at two different
installs before touching either side. When only `SUMI_LOCAL_HOME` is set,
the recorded prefix is used, so a moved install stays self-locating —
and when `config.env` is gone, the marker's recorded prefix is used the
same way, with an env prefix that disagrees refused as a mismatched pair.

`install` refuses to run while the install's service or core processes
are still alive under the target prefix — or under the previously
recorded prefix when you are moving (`config.env`, or the home marker
when `config.env` is gone). `sumi-local stop` first, then
reinstall/move: copying payloads onto running binaries fails half-way
and would strand orphans on a path the install no longer records.
`install` also refuses `--home`/`--prefix` pairs that nest inside each
other — purge removes the entire state home while promising unrelated
prefix files survive, and an overlap makes that contract unsatisfiable —
and any path containing control characters, which would corrupt the
flat `KEY=value` markers it writes. If `config.env` exists but is
incomplete or unreadable, `install` refuses rather than keep an
unstartable config: it prints the missing keys so you can repair them
in place (keeping `SUMI_LOCAL_ID`/`SUMI_PERSONA_ID` preserves the
secretary), or move the file aside and reinstall for a fresh identity.
Lifecycle commands serialize through a per-home `run/lock`, so an
`install` cannot interleave with a `stop`/`start`/`uninstall` of the
same install; unrelated installs use their own lock and run in
parallel.

If a previous install's config was deleted but its managed volume remains,
a fresh install at the same home **refuses to adopt it** — restore the old
`config.env` to keep that secretary's data, or remove the volume yourself
for a fresh start. `uninstall` refuses to remove a prefix with no
`.sumi-local-prefix` marker or a marker naming a different install,
refuses to purge a home with neither `config.env` nor the
`.sumi-local-home` marker (and a `--home` that isn't a directory), and
refuses to delete a docker volume that isn't labeled for this install's
compose project. When it does remove a prefix, it deletes only the known
payload entries — unrelated files you placed there stay. If `config.env`
is lost, `stop`/`uninstall` still stop recorded processes and the
marker-derived managed container, with docker project-label checks —
a deleted config is never treated as license to guess and remove
resources.

Copying or moving the state home copies/moves the install itself: the
recorded `SUMI_LOCAL_ID` follows the config, so the copy aliases the same
docker resources and listen address — it is the same install, not a
second one. Two copies therefore cannot *both own* a running install:
when `start` finds the configured port already serving this persona with
this install's service binary anchored on the shared prefix, it *adopts*
the running service (rebuilding the pidfile in this home), and adopts
the secretary core the same way when the writer lease names that exact
process (`local-<pid>`). A lease-free anchored core is a genuine orphan
and is swept as usual; a port held by anything not provably this install
is refused as before. So `start` from a copy while the original runs
joins the running install instead of taking it over — the copy and the
original then share one service and one secretary. To relocate, *move*
the home and update `SUMI_LOCAL_HOME` (stopped relocation is unaffected).

If the state home was deleted while the install is still present (or
still running), the CLI can no longer prove ownership: `stop` reports
"not installed", `uninstall` refuses the unverifiable pair, and nothing
is guessed. Recovery is manual but small — find the processes with
`pgrep -af <prefix>/` (their argv anchors on the payload root), terminate
them yourself, then delete the prefix directory by hand.

## Model connection

`SUMI_MODEL_PROVIDER` in `config.env`:

- `mock` (default) — echoes inputs, no credential. Includes internal tools
  (`schedule.*`, notes/files) and `!slow`/`!fail` test commands. Good for
  trying the mechanics.
- `openai` — an OpenAI-compatible chat-completions endpoint:
  `SUMI_MODEL_BASE_URL` (`…/v1`), `SUMI_MODEL_API_KEY`, `SUMI_MODEL_MODEL`,
  optional `SUMI_MODEL_HEADERS_JSON`, `SUMI_MODEL_EXTRA_JSON`,
  `SUMI_MODEL_TIMEOUT_MS`.

A ChatGPT/Codex subscription has **no built-in OAuth flow** — manual
human OAuth/subscription integration is not yet implemented. To use a
subscription-backed endpoint you must point `SUMI_MODEL_BASE_URL`/`_API_KEY`
at a bridge/proxy you run yourself that accepts your subscription
credential.

The model knobs (and lease/retry tunables) can be overridden per-start
from the environment without editing `config.env` — see
`config.example.env` for the list.

## Semantics

- `start` is idempotent: already-running healthy pieces are left alone; a
  port held by a foreign process is refused, never adopted or killed.
- `stop` terminates the secretary first (SIGTERM → graceful lease
  release), then the service, then managed Postgres. Stale pid files are
  checked against `/proc/<pid>/cmdline` before any signal; a live process
  that fails verification keeps its pidfile as evidence, and pidfiles
  inside a home that isn't proven to be an install are never deleted.
  If a pid file was *lost*, `stop`/`restart`/`uninstall` still find the
  orphaned service and core hosts by scanning `/proc` for processes
  anchored on this install's prefix — a lost record cannot strand them,
  and `restart` cannot spawn a duplicate core. That sweep only runs when
  the prefix marker still names this install, so a stale home cannot
  kill a newer install that re-claimed the same directory. If payload
  files were deleted while its processes still run (the F3 orphan
  case), the exact-path sweep can no longer see them — `stop` then
  *reports* every process with an argv element under the proven prefix
  instead of claiming a clean stop; reported paths alone never authorize
  a signal. If after that the listen port stays occupied by a process
  this install cannot verify, `stop` says so instead of claiming
  success — identify the holder with `ss -tlnp`, inspect its
  `/proc/<pid>/cmdline`, and only then terminate it if it is a leftover
  of this install.
- `uninstall --purge` asks for confirmation *before* anything is stopped
  or removed: it prints the full doomed list, and cancelling leaves
  processes, containers, and files untouched. It exits 0 even when the
  CLI shim is absent or belongs to another install, and reports anything
  it could not verify as still running rather than claiming success.
- Managed Postgres is addressed by its docker-assigned port at every
  `start`; docker re-allocates that port on container restart, so after a
  `docker restart`/`docker stop`/`docker start` of the managed container
  outside the CLI, run `sumi-local start` again to repoint a stale
  service (it restarts the service only when the DB endpoint changed).
- `status` exits 0 (up), 2 (degraded — e.g. service up but secretary not
  holding the lease), 1 (down/not installed).
- Crash recovery: if the secretary dies mid-turn, the next `start` waits
  out the dead holder's lease, recovers the interrupted work, and the turn
  completes exactly once.
- `say --id ID` reuses a caller-chosen input id; resubmitting an id is a
  durable replay (safe to retry), a conflicting resubmit is rejected.

## Security notes

- The service binds literal loopback only (`127.x`/`localhost`); the
  browser URL carries the `fm` capability credential.
- **Credentials in output**: the URL `sumi-local start` prints, and the
  service's own startup lines in `sumi-local logs` (`log/service.log`),
  contain the live `fm=`/`core_` capability tokens — that is the only auth
  the loopback surface has. Treat those outputs like a password.
  `status`/`doctor` themselves never print secret values.
- `config.env` is mode 0600 and lives under a 0700 state home.
- Before signaling a pidfile's process, `stop`/`uninstall` verify both the
  recorded `/proc` start-time and an absolute-path cmdline match anchored
  on this install's prefix — a stale or foreign pidfile can't get another
  install's process killed. The same absolute-path anchor gates the
  lost-pidfile recovery sweep, so only processes this install could have
  spawned are ever signaled.

## Distribution

```sh
sumi-local pack out.tar.gz   # from a source checkout (needs Go once)
tar -xzf out.tar.gz && cd sumi-local && bin/sumi-local install
```

A pack contains the prebuilt linux/amd64 service binary, the `core/`
TypeScript payload, this CLI, compose file, and docs. Installing a pack
needs no Go, no repository.

## Known limits / not yet

- One persona per install (the surface is single-secretary).
- Jobs/results runner and portable-state (Local→Cloud move) are being
  integrated on separate branches. `sumi-local` already forwards their
  env (`SUMI_LOCAL_EXTRA_HOSTS`, `SUMI_JOB_*`, `SUMI_RUNNER_ID`) but they
  do nothing until those hosts land on main.
- No OAuth/subscription provider integration (see above).
- No TLS — loopback only, by design for this slice.
- `say` is a convenience CLI over the same fm surface the browser uses;
  it is not the product UI.

## Fixture / self-test

`deploy/local-host/self-test.sh` runs the whole lifecycle against a
caller-supplied owned database (`SUMI_TEST_DB_URL`) with a disposable
home/prefix: install → foreign-port refusal → start → mock chat →
scheduled wake → restart persistence → SIGKILL mid-turn recovery →
OpenAI-stub provider → uninstall/reinstall identity → pack-install →
purge. `deploy/local-host/test/stub-model.mjs` is the OpenAI-compatible
stub it uses.

## Shared Local terminal

On Linux/WSL, the state service runs genuine PTY shells in
`<home>/workspace/<persona-id>`, the same Local working files served by
filesvc. Human terminal input and the secretary's `terminal.*` tools use
one session, one input ledger, and one output stream. A failing command
leaves the interactive shell and previous files available. Opening another
session uses the same directory; closing a session does not delete files.
These are ordinary processes with the Local user's permissions, not a
sandbox. The initial cwd is confined to the persona workspace; shell commands
retain the user's normal filesystem/network access. Service secrets are not
inherited into the shell environment.

The existing terminal attachment protocol is mounted at `/terminal/*`.
To attach from the existing terminal client, first POST
`/fm/<persona-id>/terminal-session` with the install's `fm_` bearer capability
(the same scope as its local chat surface). The result supplies
`installation_id`, `authority_epoch`, and `terminal_base`; pass the first two
as query parameters to the existing terminal routes. The response sets a
random 256-bit `sumi_session` cookie, scoped to `/terminal`, HttpOnly,
SameSite=Strict, expiring in one hour. Only its SHA-256 hash is stored in
memory. The configured loopback HTTP origin is the only permitted browser
origin; a Core persona bearer cannot mint this browser cookie. Expiry is
checked for requests and while attached, and host restart invalidates cookies.
The HTTP loopback host does not set Secure. No additional terminal screen
or per-command approval step is introduced.

Disconnecting the browser or restarting only the terminal driver leaves the
PTY running. A driver reclaim changes its claim epoch and reattaches to that
same shell. An input whose delivery response was lost is recorded as unknown
and is not sent again automatically. Resize changes the actual PTY dimensions;
INT, QUIT and TSTP target its current foreground process group through the
kernel terminal ioctl. Other allowed signals target the owned shell leader.
Explicit close and the eight-hour session lifetime stop the owned shell leader
and close its PTY; arbitrary detached descendants may survive.

Normal SIGTERM/INT shutdown stops the owned terminal shell leaders and records
their outcomes. An abrupt state-service process death cannot recover a PTY
descriptor. The
journal in `<home>/terminals` preserves the accepted operation identity before
launch, so recovery reports the old session as lost/indeterminate instead of
launching a replacement. It never looks up or signals a PID read from disk.
A shell that ignores hangup and detaches its stdio can physically survive:
**lost does not mean stopped**. Opening a new terminal is an explicit new
session and retains the same workspace files. Already collected Core
scrollback remains; output not collected before the host died may be lost.
Backend scrollback retains a bounded 256 KiB tail with explicit skipped-output
boundaries. Journals remain until the install data is purged.

The Local launcher provides `SUMI_WORKSPACE_ROOT` and
`SUMI_LOCAL_TERMINAL_ROOT` to the state service when Local filesvc is the
working store. For a Cloud-to-Local return that deliberately keeps Cloud
working storage, the Local PTY is unavailable: the Cloud filesystem is not
mounted as a Local directory. `doctor` states this, and terminal-session
bootstrap returns HTTP 503 with `local_terminal_unavailable` and
`working_store: cloud`. It does not silently create another workspace.

## Configure Local MCP servers

The person who owns this Local installation can grant the secretary an HTTPS
MCP connection or a Local stdio executable without a Cloud account. Create a
JSON configuration file with mode 0600, then use the executable CLI:

```sh
sumi-local mcp save /path/to/mcp.json       # create; prints connection metadata/id
sumi-local mcp list                       # metadata only, never credentials
sumi-local mcp save /path/to/mcp.json ID   # replace configuration and grant
sumi-local mcp delete ID                  # revoke and remove
```

Example HTTPS configuration (the bearer field is optional):

```json
{"name":"My remote tools","transport":"https","endpoint":"https://tools.example.com/mcp","enabled":true,"bearerToken":"server-credential"}
```

Example stdio configuration (use actual absolute executable/cwd paths):

```json
{"name":"My Local tools","transport":"stdio","command":"/usr/bin/node","args":["/home/me/tools/server.mjs"],"cwd":"/home/me/workspace","env":{"SERVER_API_KEY":"server-credential"},"enabled":true}
```

Saving with `enabled: true` is the standing grant. The secretary discovers
connections using `mcp.connections`, obtains complete tool descriptions and
schemas with `mcp.list_tools`, and invokes a discovered tool with `mcp.call`.
The latter two return durable jobs; the real result, including
`call_result.structuredContent`, is available through `job.status`. There is
no further per-call approval. Tool output cannot create or modify these
connections: only the existing persona-scoped Local `fm_` human capability
(or existing install admin authority) can use
`/fm/<persona>/mcp-connections`. Core persona tokens cannot change settings.

Configuration is encrypted in PostgreSQL and bound to both `SUMI_LOCAL_ID`
and the configured persona. Its key is derived under a distinct domain from
the installation's existing secret. Closing the browser or restarting normally
does not remove it. Another Local installation or secretary cannot discover
or use that grant. These host configurations and credentials are not portable
Core state: another host, a changed installation ID, or a changed install
secret requires reconfiguration. API/CLI listing returns metadata only;
replacement therefore needs the complete configuration, including credentials.
Keep your configuration file private or remove it after saving.

Each discovery/call starts a stdio protocol server for that bounded job, using
the official MCP SDK's newline-framed transport, and stops it when the job
finishes. The process inherits only a fixed PATH and LANG plus explicitly
configured environment values; it does not inherit the host's database,
model, or admin environment credentials. It runs with the Local user's normal
permissions and the configured cwd; it is not a sandbox. The executable is
explicit, not a shell command, and no package is downloaded automatically.
Noisy stderr is discarded, each inbound frame is bounded to 2 MiB, and the
operation has a 30-second deadline. Notifications can be omitted with an
explicit marker to keep the primary result; genuinely oversized primary
results are explicitly omitted rather than returning a partial schema.

Local stdio uses a host-specific job kind; the Cloud MCP runner does not
claim it, and generic job submission cannot supply executable configuration.
Cancellation, normal host SIGTERM/INT, initialization failure, and completion
close pipes and kill the owned process group before reaping its leader. This
retains the group's identity during cleanup rather than signaling a reused
PID. Descendants that deliberately leave that process group remain outside
this lifecycle boundary. SIGKILL/power loss cannot run that cleanup; Linux
parent-death signaling stops the direct child but is not proof that all
children stopped. No physical-quiescence claim is made.

An already started server can have initialization side effects even when no
tool call was dispatched; the result distinguishes `server_started` from
`dispatched`. A tool mutation whose response is lost is indeterminate and is
never automatically sent again. Interrupted claimed jobs expire to lost;
inspect the affected external state before deliberately issuing a new call.
Revocation prevents queued work and future dispatch using the prior grant;
an operation already admitted is bounded and finishes before revocation
returns. An MCP error does not end the secretary's Local terminal or delete
its workspace files. HTTPS retains the main API's public-destination checks,
credential handling and protocol semantics; stdio does not enable private
HTTP destinations. OAuth, MCP Apps, macOS support and a new onboarding UI
are not part of this interface.
