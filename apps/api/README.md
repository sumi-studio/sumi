# Sumi Go API

The Todo API is contract-first: update `../../contracts/openapi.yaml` and regenerate
`@sumi/api-client` before changing the Go wire behavior.

## Todo database

### Docker Compose

From the repository root, generate local-only credentials and start the complete
PostgreSQL → migration → API → Caddy stack:

```sh
make compose-env
make compose-up
curl http://localhost:8080/health
```

The generated `.env` is mode `0600` and ignored by Git. Compose initializes a
development `users` table and the UUID
`019c0000-0000-7000-8000-000000000001`, then applies the embedded Todo migration.

To exercise the authenticated Todo API:

```sh
cookie="$(make -s compose-session)"
curl -sS -b "$cookie" \
  -H 'Content-Type: application/json' \
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

`make compose-down` preserves PostgreSQL/API/Caddy volumes. Use
`docker compose down --volumes` only when intentionally resetting all local data.
For the EC2/Caddy deployment, set `SUMI_SITE_ADDRESS` to the public hostname and
use host ports `80`/`443`; do not deploy the generated local `.env`.

### Direct execution

The Todo migration expects the identity/control-plane schema to already provide:

```sql
CREATE TABLE users (user_id UUID PRIMARY KEY);
```

Set `SUMI_DATABASE_URL` to a PostgreSQL connection string, then run:

```sh
go run ./cmd/migrate
go run ./cmd/server
```

`SUMI_DEFAULT_TIMEZONE` is optional and defaults to `Asia/Tokyo`. The server derives
`owner_user_id` only from the signed `sumi_session` and requires that claim to be an
internal UUID. Every Todo repository operation includes that owner in its SQL predicate.

The Go `AgentTools` adapter exposes `list_todos`, `get_todo`, `create_todo`,
`update_todo`, and `propose_delete`. It deliberately provides no `delete_todo`; only
the authenticated HTTP DELETE route can perform the physical deletion after UI
confirmation. An HTTP tool client uses the same signed user session and sends
`X-Sumi-Via-Agent: true` on POST/PATCH. This header is informational only and never
changes ownership or authorization.
