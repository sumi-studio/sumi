# Sumi Go API

The Todo API is contract-first. Update `../../contracts/openapi.yaml`, regenerate
`@sumi/api-client`, and keep the Go wire behavior aligned with that contract.

## Todo backend

Todo storage uses the shared control-plane PostgreSQL database. Migration
`internal/db/migrations/0005_todos.up.sql` creates `todos.owner_user_id` as a
foreign key to the canonical `humans.human_id`; no Firebase UID or browser input
is stored as the owner.

Todo routes are disabled by default. They are registered only when all of the
following are configured:

- `SUMI_TODO_ENABLED=true`
- `SUMI_DB_URL`
- the complete browser-session authentication configuration

The server-issued `sumi_session` supplies the HumanId. Every repository query is
owner-scoped, and every operation runs while the browser-session authorization
lease is held so logout cannot race a Todo mutation. POST, PATCH, and DELETE also
require `X-Sumi-CSRF: 1`, same-origin browser metadata, and JSON where applicable.
Responses are marked `private, no-store` and `Vary: Cookie`.

The supported Vite development server proxies `/v1` to the API without changing
the browser Host. A Todo frontend should therefore call relative `/v1/todos`
URLs; direct cross-origin browser calls are intentionally rejected.

## Verification

Start the shared development PostgreSQL service and run migrations:

```sh
make db-up
SUMI_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:5432/sumi?sslmode=disable make migrate
```

Run the Go suite and the explicit PostgreSQL Todo contract test:

```sh
cd apps/api
go test ./...
SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:5432/sumi?sslmode=disable \
  go test ./internal/todo -run TestPostgresTodoContract -count=1
```

## Agent boundary

`AgentTools` defines `list_todos`, `get_todo`, `create_todo`, `update_todo`, and
`propose_delete`. It deliberately provides no physical delete operation and is
not registered in the agent runtime yet.

Do not register this adapter until action-scoped authority and idempotency
receipts are implemented. In particular, an authenticated browser session or a
client-supplied marker is not agent authority. Human HTTP requests always record
`via_agent=false`; a future authority adapter will invoke the same application
service through `AgentTools` after validating delegation, action, target,
generation, expiry, and request identity.
