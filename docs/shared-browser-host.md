# Connect a secretary to a shared browser tab

The secretary can now discover, observe and act on an explicitly granted tab in
Sumi's Electron runtime through `browser.tabs`, `browser.observe` and
`browser.act`. It uses the same `WebContentsView` the person sees. The Go API
checks the standing grant and records each request as a Core job; the desktop
host polls outbound, performs the operation once, and posts its receipt. There
is no second browser or inbound desktop automation server.

This is a configured engineering entry. Desktop sign-in/onboarding and product
browser chrome are not implemented. The host must already possess the person's
valid Sumi browser session for the initial attachment. The page being browsed
never receives that session or the separate host credential.

## Configured launch

Run API migrations through `0065_browser_tabs`; the normal API server wires the
browser attachment routes, tool effects and orphan-job sweeper whenever the Core
state service is configured. No extra browser service token is needed by the API.

Create an owner-only JSON file outside the repository (mode `0600` on Unix),
using the actual current Sumi session, CSRF token and configured permitted
Origin. Placeholders below are not valid credentials:

```json
{
  "apiOrigin": "https://your-sumi-api.example",
  "humanHeaders": {
    "Origin": "https://your-sumi-app.example",
    "Cookie": "sumi_session=<current session>; sumi_csrf=<current csrf token>",
    "X-CSRF-Token": "<same current csrf token>"
  },
  "personaId": "<your secretary's persona UUID>",
  "profileId": "my-browser-profile",
  "name": "My research tab",
  "url": "https://example.com",
  "allowActions": true
}
```

From the repository root:

```sh
pnpm --filter @sumi/desktop install --frozen-lockfile
SUMI_BROWSER_HOST_CONFIG=/absolute/path/to/owned/browser-host.json \
  pnpm --filter @sumi/desktop dev:browser:connected
```

On WSL without WSLg, put `xvfb-run -a` before `pnpm`; Xvfb is useful for automated
acceptance, while an actual display is needed for a person to use the window.
HTTP is accepted only for loopback Local API origins. Redirects are refused.
The config requires Node >=22.18 and the ordinary Electron platform libraries.

The entry opens the real tab, obtains an attachment using the person's current
session, and begins authenticated polling. It logs the attachment ID/name/tab
identity, **not** credentials. The in-memory bridge retains only the tab-scoped
host token, not the login-header object. The config file remains owned by its
operator and must be removed or updated when no longer needed. No real user's
credential was used in this implementation's tests.

The API's normal Origin, session and CSRF checks apply to attachment management.
No new authentication bypass was added. `allowActions: false` grants observation
only; `true` grants the bounded action operations too. Each grant names exactly
one active secretary owned by that person and one browser runtime/profile/tab
incarnation. This slice permits only one enabled attachment per person's live
tab; revoke it before granting that tab again. A new process/tab needs a new
attachment. It never inherits the authority of an old runtime ID.

## API and tool contract

Human session/Origin/CSRF authentication:

- `POST /api/browser-tabs` with `{persona_id, name, tab, allow_actions}` creates a
  standing grant and returns `{attachment, host_token}` once. `tab` is the actual
  browser-owned `{runtimeId, profileId, tabId}`, not a website-provided ID. Only
  SHA-256 of the randomly generated host token is persisted.
- `GET /api/browser-tabs` lists that person's enabled attachments without tokens.
- `DELETE /api/browser-tabs/{attachment_id}` revokes that person's attachment.

Host bearer credential, never a Core/persona credential:

- `POST /api/browser-host/tabs/{id}/poll` atomically authorizes and consumes at
  most one queued operation for that exact attachment. It also renews the
  attachment's presence timestamp. The route refuses browser `Origin` headers;
  it is called by privileged main-process code.
- `POST /api/browser-host/tabs/{id}/complete` posts
  `{job_id,status,result,error}`. The server checks the host, attachment, persona,
  job kind and recorded claim owner. Completion receipts for already dispatched
  work are accepted after revocation, so its outcome is not discarded.

Secretary tools use the existing durable operation/result path:

1. `browser.tabs` returns accessible names, exact tab identity, `attachment_id`,
   availability and action permission. No ID needs to be guessed. Availability
   means a valid host poll was received within 30 seconds, not a promise that a
   window cannot close immediately afterward.
2. `browser.observe({attachment_id})` queues a browser job. After its completion
   input, `job.status({job_id})` returns `result.value` containing the observation.
3. `browser.act({attachment_id,binding,action})` queues one operation bound to the
   observation. `action` is click, fill, scroll or navigate as described by the
   runtime contract. Read its job outcome, then observe the page again to verify
   what the website did.

Grant checks apply before both observation and action admission and again when
an authenticated host consumes the job. A standing action grant does not add a
per-operation approval prompt. A model may still explicitly choose the
existing elevated route. General Core job runners cannot claim browser jobs.
No host token, human cookie, arbitrary JavaScript or raw CDP enters model input.
Page text remains untrusted website data; observation results are durable Core
records, visible to the secretary whose grant authorized them.

## Disconnects, navigation, revocation and unknown effects

A host dispatch claim lasts 30 seconds. The main-process bridge validates the
exact tab reference and a nonexpired claim before calling the runtime. Runtime
operations remain bounded to five seconds. Host/API clocks must be sufficiently
aligned for the expiry guard; a clock ahead fails dispatch rather than extending
its grant. No command is rebound to a different tab/profile/process.

Revocation is serialized with dispatch admission. It prevents subsequent Core
admission and host dequeue. **An operation already admitted for dispatch may
still execute or finish after revocation returns.** Revocation does not retract
a prior observation from the secretary's durable history. Queued operations that
cannot be dispatched fail after 60 seconds with `dispatched:false`. A host that
has not polled for 30 seconds is shown unavailable and new operations are refused.

The bridge never repeats a possibly dispatched action. If a poll response is
lost after the server consumed the command, it becomes `lost` when its claim
expires; a later poll does not retrieve it again. If the runtime returns but the
completion response is lost, only the identical receipt is retried. If the host
process dies before persisting a receipt, the job eventually becomes `lost` with
an indeterminate outcome. The one-second API sweeper records these outcomes and
creates the existing `job_completed` input; no continuing model process is needed.

A returned runtime error is stored with its code (for example
`stale_observation`, `tab_closed`, `runtime_unavailable`) and a conservative
`unknown` outcome if dispatch was attempted. It is not proof that nothing changed.
The secretary should inspect current page state before proposing a new action.
Website completion is also not implied by a successful `dispatched` receipt.

## Acceptance and limits

The tests use real PostgreSQL, HTTP API, Secretary planning/claims/results, a
scripted deterministic model provider, actual sandboxed Electron, a real local
website, native Chromium input and a screenshot of the attached view. The
secretary discovers the tab from `browser.tabs`, reads human-path text, fills and
clicks through durable jobs, then observes the same visible result and one click.
The login proof in the integration fixture alone is substituted with a test
header; production routes still use the real existing session/CSRF authorizer.
This does not claim end-to-end desktop sign-in or hosted deployment acceptance.

```sh
pnpm --filter @sumi/desktop test
pnpm --filter @sumi/agent-core check-types
cd apps/api
SUMI_TEST_DB_URL='postgres://<owned disposable test database>' \
SUMI_TEST_DB_PREFIX=sumi_browser_core_ \
SUMI_BROWSER_REAL_TEST=1 \
SUMI_BROWSER_TEST_ARTIFACTS=/absolute/path/to/owned/test-artifacts \
  go test ./internal/browsertabs -count=1 -v
```

The database helper creates and drops uniquely named isolated databases; use an
owned test Postgres server, never production. Electron must have been built by
`pnpm --filter @sumi/desktop build` before the real-browser Go test. Xvfb must be
installed on Linux. Host HTTP fault-injection unit tests separately prove that
response loss retries receipts, not clicks, and mismatched tab identities fail.

No new packages are needed beyond the existing Electron/runtime dependencies.
No live-tab restart restoration, native sign-in UI, Mac/Windows packaging or
validation, Jev integration, arbitrary-page trusted-input compatibility,
child-frame interaction, full accessibility tree or browser product chrome was
added. See the [runtime limits](../apps/desktop/README.md) for the DOM operation
scope. The API/host bridge is implemented; external hosted-network deployment
has not been exercised in this acceptance.

### Cancel a browser job

Use the existing `job.cancel({job_id})` tool. Cancellation records what can still
be stopped; it never undoes a website operation.

| Point reached | Recorded result | Meaning |
| --- | --- | --- |
| Cancel wins before host dequeue | `cancelled`, `claimed_by:null`, `started_at:null`, `result:null` | No host received this command. It cannot subsequently be dequeued. |
| Host already consumed the command | `cancel_requested`; result may still be null | Dispatch was admitted. The short browser operation may already be running or may still run; no hard abort is promised. |
| Host reports completion after cancellation | Actual `done`/`failed`, with `cancel_requested_at` retained | Inspect `result.dispatched`, `result.outcome` and the returned value/code. Successful completion is not rewritten as if cancellation undid it. |
| Claimed command loses its result and expires | `lost`, preserving cancellation timestamp and claim identity | Its effect is indeterminate. A missing `dispatched` field is **not** `false`; never blindly repeat it. |

A cancelled queued command does not block later work on the tab. An admitted
cancel-requested command retains the tab's execution slot until its receipt or
30-second claim expiry, so cancellation does not allow another operation to race
an in-flight click. Expiry never requeues that click. When the authenticated host's
late receipt arrives after the job was already swept `lost`, the API attaches
what the host actually observed under `result.observed_outcome` — the `lost`
verdict and its single terminal notification stand, the record is enriched, never
rewritten. An identical receipt replays the stored row; a divergent one still
conflicts (HTTP409), and the host discards that receipt and continues polling for
distinct later commands. HTTP403 still stops polling because the attachment is
no longer authorized. A lost response to an already committed completion only
causes replay of the identical receipt, with one terminal notification.

### Host network authority

A granted shared tab uses the browser host's ordinary HTTP(S) network reachability,
including local and private sites the host can reach. This is intentional for the
application people and their secretary share. The standing tab grant permits
observing those pages and, when actions are enabled, navigating to them; it does
not add a separate approval for each private destination. This is distinct from
the API's server-side public-web/MCP proxy, which restricts private-network egress.
Chromium origin and session isolation still apply. Non-web privileged protocols
remain blocked, and malformed request URLs are cancelled with a completed callback.
