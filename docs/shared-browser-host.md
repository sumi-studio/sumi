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

Run API migrations through `0067_browser_goal`; the normal API server wires the
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
  "allowActions": true,
  "jev": {
    "apiKeyFile": "/absolute/path/to/owned/typesafe-api-key",
    "model": "jev-latest"
  }
}
```

`jev` is optional; omit it for direct browser use only. See
[Delegate a goal to Jev](#delegate-a-goal-to-jev).

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
validation, arbitrary-page trusted-input compatibility,
child-frame interaction, full accessibility tree or browser product chrome was
added. See the [runtime limits](../apps/desktop/README.md) for the DOM operation
scope. The API/host bridge is implemented; external hosted-network deployment
has not been exercised in this acceptance.

## Delegate a goal to Jev

A secretary can hand a purpose, rather than each click, to the
[TypeSafe Jev](https://docs.typesafe.ai/) operation layer on the same granted
tab. Jev is a decision model: it returns typed choices with probabilities, not
generated text. It does not replace the person's chosen main model or the
secretary; the secretary decides when to delegate, supplies the values, and
verifies the outcome.

**Configure (host only).** Save the person's TypeSafe API key in its own
owner-only file (`chmod 600`, at most 4 KiB) and reference it from the host
configuration's `jev.apiKeyFile`. `jev.model` defaults to `jev-latest`; pin a
versioned id such as `jev-1.13.0` to keep behavior fixed. `jev.endpoint`
defaults to `https://api.typesafe.ai` (HTTP is accepted only on loopback, for
owned test doubles). `jev.minConfidence` (0–1, default 0.3, untuned) stops a
goal as `uncertain` instead of acting on a weak choice. The key is read into the
Electron main process only. It is never put in the environment, the website,
`apps/web`, the Sumi API/DB, logs or job results. An unreadable or malformed key
file logs `Jev operation layer disabled: …` and the host continues with the
direct path.

**Availability.** Each host poll declares whether Jev is configured.
`browser.tabs` returns `operation_layers: {direct: true, jev: "available" |
"not_configured"}`. `browser.goal` requires the action grant and `jev:
"available"`; otherwise it is refused before anything is queued with "the Jev
operation layer is not configured on this tab's browser host; use
browser.observe and browser.act directly". No other model is substituted.

**Call.**

```json
{"attachment_id": "…", "goal": "Sign up for the newsletter with my name and email, then save.",
 "inputs": {"name": "Ada Lovelace", "email": "ada@example.test", "password": "…"},
 "private_inputs": ["password"], "max_steps": 15}
```

The host claims the job like any browser job and loops on the same tab:
observe → one `POST /v1/systemone` request (operation choice plus click-target,
field/input and URL-input choices) → admission → guarded act. Operations are
click (links, buttons, checkbox/radio), fill a text field with one of the
`inputs`, open an `inputs` value that is an http(s) URL, scroll, wait, DONE and
BLOCKED. Jev never supplies text or URLs. Exact comparisons (does a field
already contain an input?) are done in code. Jev receives the goal, the
non-private input values, the page's visible text (≤6000 chars), the control
table and the last eight steps; private input values are typed but replaced by
`(private value, not shown)`. Input values are stored in the durable job request
like direct `fill` text.

**Progress and stop.** Before every action and every 10 s the host posts
`POST /api/browser-host/tabs/{id}/progress`. The API renews the 30 s claim and
tab presence and stores `result.progress` (phase, step, actions dispatched, last
step, next action, URL/title, Jev call count), which `job.status` shows while
running. It answers with the job status: after `job.cancel` the goal stops
before its next action and completes `cancelled`; after revocation the progress
call is refused (403) and the goal completes `failed` / `grant_revoked`. An
action already admitted may still land; nothing is undone. Progress is refused
after 6 minutes (the host's own budget is 5 minutes), for non-goal jobs, and for
lost/finished jobs.

**Person and page changes.** Each action uses the observation Jev decided on
and the runtime guard: native person input on the tab or a changed observed
control after that observation refuses the action with `page_changed`. The loop
re-observes and asks Jev again; three consecutive refusals end the goal as
`page_changed_repeatedly` so the person or secretary can take over.

**Result.** The receipt keeps the browser shape `{dispatched, outcome, value,
code?}` and the late-receipt/lost semantics above. `value` has
`operation_layer: "jev"`, `goal_outcome`, `reason`, `actions_dispatched`, each
step (operation, target name, input name, result such as `dispatched` or
`refused:page_changed`, confidence), `jev` (response model id, call count,
tokens, last decision's top probabilities) and `final_page`. Job status is
`done` for `jev_reported_done`, `blocked`, `uncertain`, `step_limit`,
`time_limit`, `no_progress` and `page_changed_repeatedly`; `cancelled` for a
cancellation; `failed` with `code` for Jev errors (`jev_auth_failed` 401/403,
`jev_rate_limited` 429, `jev_overloaded` 408/5xx incl. 529,
`jev_request_rejected` 422, `jev_unreachable`, `jev_invalid_response`),
`jev_not_configured`, `grant_revoked`, `claim_lost`, `api_unreachable` or a
browser error. 408/429/5xx and connection failures are retried twice with
backoff (honoring `retry-after` up to 5 s), like the official SDKs.
`jev.calls` counts only calls that returned answers. `jev_reported_done` is
Jev's judgment; the secretary should `browser.observe` to verify.

**Scope.** Same DOM operations as the direct path: top-level document only; no
frames, shadow DOM, select menus, uploads, drag/drop, canvas or custom widgets.
Jev-1.13 is strongest in English and is documented as vulnerable to adversarial
page text; the page is marked untrusted in every question, only supplied inputs
can be typed, and the step budget bounds a misled goal, but the standing action
grant is what authorizes its clicks.

**Acceptance and limits.** `pnpm --filter @sumi/desktop test` covers the
adapter, decision space and loop against a contract-checking loopback Jev test
double. `xvfb-run -a pnpm --filter @sumi/desktop test:browser:jev` runs the
actual Electron journeys. `TestSecretaryJevGoalRealBrowser` (same env as the
real-browser test above) runs Secretary → API/DB → host → the double → the same
tab. None of these used the live TypeSafe API; with a key, the live check is the
connected launch above plus a `browser.goal` from the secretary.

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
