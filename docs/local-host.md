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
  macOS/other Unix may work as a low-trust fallback — pid safety uses
  `/proc`, which is Linux-only. Native Windows is unsupported; use WSL.
- bash ≥ 5, `curl`, `openssl`, `flock`, `tar`, `nohup`
- Node ≥ 22.18 or ≥ 23.6 (runs `.ts` directly; `sumi-local doctor` checks)
- PostgreSQL, either:
  - `--db-url postgres://…` to a database you provide, or
  - `--managed-pg` (default when Docker is available): a dedicated
    loopback-only `postgres:17-alpine` container + named volume per
    `deploy/local-host/compose.pg.yaml`. Not a shared system service —
    `restart: "no"`, started/stopped by `sumi-local`.
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

Installing drops a `sumi-local` shim in `~/.local/bin`.

## Layout

| Path | Contents | Survives `uninstall`? |
| --- | --- | --- |
| `~/.local/lib/sumi-local` (`--prefix`) | executables: service binary, `core/` TypeScript sources, CLI | no — removed |
| `$XDG_STATE_HOME/sumi/local` (`--home`) | `config.env` (0600: identity, secrets, model), `run/` pids, `log/`, `workspace/` | yes — `--purge` to delete |
| `~/.local/bin/sumi-local` | CLI symlink | no — removed |
| managed PG volume | canonical database (managed mode) | yes — `--purge` deletes it |

Identity lives in `config.env`: `SUMI_PERSONA_ID` is generated once at
first install and preserved on reinstall — that is what makes the
reinstalled secretary *the same individual* with the same history.

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
  checked against `/proc/<pid>/cmdline` before any signal.
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
- `config.env` is mode 0600 and lives under a 0700 state home.
- `status`/`doctor` never print secret values.

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
