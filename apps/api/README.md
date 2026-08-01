# Sumi Go API

The Todo API is contract-first: update `../../contracts/openapi.yaml` and regenerate
`@sumi/api-client` before changing the Go wire behavior.

## Todo database

### Docker Compose

From the repository root, generate local-only credentials and start the complete
PostgreSQL → migration → API stack:

```sh
make compose-env
make compose-up
curl http://localhost:8080/ready
```

The generated `.env` is mode `0600` and ignored by Git. Compose applies the
embedded Todo migration to PostgreSQL, enables the Todo backend, and explicitly
opts into the temporary development session adapter. Production Todo routes stay
disabled until user-scoped authentication is implemented.

To exercise the authenticated Todo API:

```sh
cookie="$(make -s compose-session)"
curl -sS -b "$cookie" \
  -H 'Content-Type: application/json' \
  -H 'X-Sumi-CSRF: 1' \
  -d '{"title":"Composeで作ったTodo","due":{"kind":"date","date":"2026-08-01","timezone":"Asia/Tokyo"}}' \
  http://localhost:8080/v1/todos
curl -sS -b "$cookie" http://localhost:8080/v1/todos
```

The complete curl example creates, lists, completes, conflict-checks, and deletes
one Todo:

```sh
./scripts/dev/todo-api-curl-example.sh "請求書を送る"
# or: make compose-todo-smoke
```

Set `SUMI_KEEP_SAMPLE_TODO=1` to skip the final DELETE.

`make compose-down` preserves PostgreSQL/API volumes. Use
`docker compose down --volumes` only when intentionally resetting all local data.
Compose exposes the API only on `127.0.0.1` by default. Production TLS termination
and public routing are outside this local stack; do not deploy the generated `.env`.

### Direct execution

Set `SUMI_DATABASE_URL` to a PostgreSQL connection string, then run:

```sh
go run ./cmd/migrate
go run ./cmd/server
```

Set both `SUMI_TODO_ENABLED=true` and `SUMI_TODO_DEV_SESSION_AUTH=true` only for
local backend development. Without `SUMI_TODO_ENABLED=true`, the conversation API
starts without PostgreSQL and does not register `/v1/todos`. The migration can run
against a clean database; it stores the internal owner UUID without creating or
requiring the control-plane `users` table. Production authz must validate that
owner before Todo routes are enabled with a future user-scoped verifier.

`SUMI_DEFAULT_TIMEZONE` is optional and defaults to `Asia/Tokyo`. The server derives
`owner_user_id` only from the signed `sumi_session` and requires that claim to be an
internal UUID. Every Todo repository operation includes that owner in its SQL predicate.
POST, PATCH, and DELETE also require `X-Sumi-CSRF: 1`; JSON mutations require
`Content-Type: application/json`.

The Go `AgentTools` adapter exposes `list_todos`, `get_todo`, `create_todo`,
`update_todo`, and `propose_delete`. It deliberately provides no `delete_todo`; only
the authenticated HTTP DELETE route can perform the physical deletion after UI
confirmation. Human HTTP requests always record `via_agent=false`; callers cannot
self-assert agent provenance. A future authenticated agent authority adapter will
invoke the application service through `AgentTools`, which records agent writes
without trusting a browser-supplied marker.
