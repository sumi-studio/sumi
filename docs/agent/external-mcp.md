# Remote MCP tools

The main Go API can give a person's secretary access to remote MCP tools. The
server requires PostgreSQL, the Core state service and `SUMI_MODEL_CONNECTION_KEY`
(the same 32-byte base64 key used to seal model credentials). MCP ciphertext has
its own authenticated-encryption domain. With no key/Core service, the connection
API returns unavailable and the MCP tools are not advertised.

## Configure a connection

These endpoints use the existing signed-in browser session and origin checks.
Mutations revalidate the initiating session using the existing authorization
callback. There is no new settings screen.

| Method | Path | Result |
|---|---|---|
| GET | `/api/mcp-connections` | This person's connections, never their credentials |
| POST | `/api/mcp-connections` | Create a connection |
| PUT | `/api/mcp-connections/{id}` | Replace the person's connection and grant |
| DELETE | `/api/mcp-connections/{id}` | Remove the person's connection and grant |

POST/PUT accept:

```json
{
  "name": "My tools",
  "endpoint": "https://tools.example.com/mcp",
  "enabled": true,
  "bearerToken": "server-issued-secret"
}
```

`enabled: true` grants the secretary bound to this human access to that
connection's remote tools, including tools that change remote state. It is an
explicit connection-level grant; MCP annotations do not grant additional
permissions. `enabled: false` revokes it. PUT is a replacement: resubmit the
credential, or use an empty bearerToken for a server requiring no authentication.
Every update changes the grant version, so already queued calls under the old
configuration fail without remote dispatch. A foreign connection returns 404.
Endpoints must be HTTPS without embedded credentials, query or fragment. DNS
answers are checked at each new connection against the same public-address
policy as Sumi's public-web fetcher, then the checked IP is dialed directly.
Private addresses, proxies and HTTP redirects are not allowed in production.

## Secretary interaction

The Core exposes three delegated tools only when this backend is registered:

1. `mcp.connections` returns enabled connection IDs and names for the secretary's
   bound human. It does not expose endpoints or credentials to the model.
2. `mcp.list_tools({connection_id, cursor?})` starts a durable job to fetch one
   page of tool descriptions and input/output JSON schemas. Its result contains
   `tools` and `next_cursor`.
3. `mcp.call({connection_id, name, arguments})` starts a durable job for one
   remote invocation. Its result contains the MCP `call_result`, including
   content, structuredContent and isError, plus `protocol_version` and outcome.

Both asynchronous tools return a job ID. The normal `job_completed` input tells
the secretary when the job ends; `job.status` reads the structured result. The
secretary's existing operation receipt prevents a recovered turn from queuing the
same operation twice. Normal-route calls use the connection grant; an explicitly
elevated call still uses the existing one-shot approval mechanism.

At actual execution the worker rechecks active persona ownership, the enabled
grant and its version. It locks that authorization through the bounded remote
operation. Revocation may wait for an already executing operation to finish;
when revocation returns, queued work under the old grant cannot start. Revocation
cannot undo an effect the server already performed. Cancellation while a request
is in flight closes the request and may leave its remote outcome uncertain.

The worker opens a fresh Streamable HTTP session for each job, initializes the
protocol, sends initialized/session/version headers, and closes the session.
Before a call it fetches the current schema (at most 32 pages), validates the
arguments, then calls once. Remote JSON Schema references are refused rather
than fetched. Structured output is checked against the server's output schema
when present. Tool errors and malformed output retain the remote content and
make the job fail; they do not claim the remote effect was undone.

No remote mutation is automatically retried. A response lost after dispatch is
recorded as `outcome: indeterminate` with an explicit warning to inspect remote
state before issuing a new call. A dead worker's expired claim becomes `lost`
and sends one durable notification, never requeues. Completion persistence may
be retried with the same observed result. Recognized bearer-token echoes and NUL
characters are scrubbed from remote results; credentials are never included in
job requests, connection listings or application logs.

## Bounds and present limits

- Transport: Streamable HTTP, with JSON and SSE response bodies. Legacy HTTP+SSE
  transport, stdio, remote OAuth, sampling, elicitation, roots, resources/prompts,
  MCP Apps rendering and MCP tasks are unsupported. Returned metadata remains
  data; no HTML surface is activated.
- Official Go SDK v1.3.1 is pinned to keep the existing Go1.23 baseline. v1.4.1
  and v1.8.0 require Go1.25. The acceptance server negotiates `2025-06-18`;
  newer-only protocol servers are not claimed as supported. Initialization
  rejects a version the pinned SDK does not support.
- Sessions live only for a job. Progress and tools/list_changed notifications
  observed during that session are retained (up to 16) in its result, not live
  pushed UI events. There is no subscription while idle. Discovery is refreshed
  for every call, so tool changes do not rely on cached notifications.
- A remote operation has a 30-second context; session cleanup also has bounded
  HTTP timeouts. One worker processes one job at a time with fair persona
  rotation. Arguments are at most 32 KiB, transport bodies 2 MiB, durable results
  60 KiB. Oversized results are explicitly omitted, never silently shortened
  into a supposedly complete result.
- This is wired into `cmd/server`. The minimal standalone Local host state
  service has no signed-in connection-configuration API and is not wired by this
  slice. Connection credentials are not portable secretary state; moving to a
  different installation requires configuring a connection there.

## Executable acceptance

Against an isolated test PostgreSQL server and Node22.18+:

```sh
cd apps/api
SUMI_TEST_DB_URL='postgres://user:password@127.0.0.1:5432/test?sslmode=disable' \
SUMI_TEST_DB_PREFIX=sumi_mcp_ go test ./internal/mcpconnections -v
```

The tests create and drop their own databases. They configure connections through
the real HTTP API, run the actual TypeScript Secretary using a deterministic
provider, traverse the real state HTTP API/operation ledger and PostgreSQL jobs,
invoke an actual SDK HTTP MCP server and read its structured result back through
`job.status`. The browser identity verifier is substituted with fixture
identities; Firebase/browser sign-in and paid third-party servers are not tested.
Other cases cover auth/ownership, schema rejection, pagination, credential echo
redaction, remote errors, invalid output, protocol rejection, update/delete
revocation, response loss, restart without redispatch, and expired claims.
