# MCP tools

The main Go API can give a person's secretary access to remote MCP tools. The
server requires PostgreSQL, the Core state service and `SUMI_MODEL_CONNECTION_KEY`
(the same 32-byte base64 key used to seal model credentials). MCP ciphertext has
its own authenticated-encryption domain. With no key/Core service, the connection
API returns unavailable and the MCP tools are not advertised.

The account-free Local host separately supports HTTPS and explicitly configured
stdio servers through `sumi-local mcp`, using its existing persona-scoped human
capability. See [Local MCP configuration](../local-host.md). The signed-in routes
below describe the main API; Local credentials and executable configuration stay
with the Local installation.

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
2. `mcp.list_tools({connection_id, cursor?, names?})` starts a durable job to
   fetch one page of tool descriptions and input/output JSON schemas. Every
   definition in `tools` is whole: a schema is never shortened, because a
   shortened schema invites a guessed call. A page therefore holds as many
   whole definitions as the durable bound allows, and the rest of the result
   says how to reach the others — `next_cursor` to pass back as `cursor`,
   `next_names` listing what is still waiting on this page, and
   `remaining_on_page`. Passing `names` fetches exactly those definitions
   instead of paging to reach them (not combinable with `cursor`), reporting
   `names_not_found`. A tool whose own definition cannot fit a page is named in
   `tools_omitted` with its stored size and the reason; paging continues past
   it. A definition containing a bearer token or declared private Local value
   is also omitted as unavailable, never offered as a mangled callable schema.
   `page_changed` says the server's list changed under a cursor and that
   discovery restarted at page zero (including changes in earlier consumed
   pages), so definitions may be re-shown, and
   `scan_truncated` says a bounded scan stopped before the server's list ended.
   In the last resort — a result that cannot be stored at all — `result_omitted`
   is paired with `repeat_request` and no cursor, because a cursor past
   schemas that were never delivered would skip them silently.
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
  transport, Cloud stdio, remote OAuth, sampling, elicitation, roots, resources/prompts,
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
  60 KiB. Progress and tools/list_changed events are expendable: at most 16 are
  kept, each message is bounded, and `notifications_dropped` counts the rest.
  If they would still displace the primary result they are discarded wholesale
  with `notifications_omitted`. A primary result that is itself too large is
  explicitly omitted; schemas are never silently shortened.
- A stored result passes through one transformation: NUL replacement (jsonb
  cannot hold a NUL) and redaction of bearer tokens and explicitly selected
  private Local argument/environment values. Ordinary config is not a secret.
  Local `privateEnv` selects environment names and `privateArgs` selects
  zero-based argument indexes; see the Local configuration contract.
  `mcpconnections.persist` is that boundary, and it draws
  one line. A result's own top-level keys, and the value of `next_cursor`, are
  written by this package — the cursor from a page number, an offset and a
  digest of names on that page and all earlier pages, never from remote bytes —
  and are left alone; everything beneath them
  is the server's and is traversed in full, keys included. That is also why a
  continuation cursor never carries the server's own cursor: doing so would
  hand remote bytes back through a reversible encoding where redaction could
  not see them. Discovery measures its pages *through* that same function, so
  the size a page is decided on is the size that is stored.
- Continuations detect changes in page name sequences and boundaries while
  re-walking at most 32 pages within the job deadline, restarting at page zero
  with `page_changed`. They are not snapshots: concurrent changes within one
  walk or schema-only changes can escape detection. A continually changing
  list may need a fresh discovery request. Old cursor formats are refused;
  restart without a cursor.
- A definition containing a protected value anywhere (including descriptions
  or schema keys) is unavailable. Its omission has a scrubbed display name,
  not a callable alias, and calls are refused before dispatch. Correct the
  server definition or configuration marking before using it. Literal secret
  echoes in results, errors and progress are scrubbed; transformed/encoded
  secrets are not automatically recognized.
- The main API route is wired into `cmd/server`; Local uses its own
  fm-authorized configuration route and host-scoped runner in `cmd/first-model`.
  Connection credentials are not portable secretary state; moving to a
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
