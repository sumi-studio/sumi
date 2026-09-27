# Cloud shared browser (`sumi-browser`)

The secretary's browser workspace in Web Sumi (`/browser`, rail item
「秘書のブラウザ」). One remote Chrome per profile runs on Cloudflare Browser
Run; the person watches and drives its actual pixels (CDP screencast at the
remote viewport, 1280×800), and the secretary uses the same tab through the
ordinary `browser.observe` / `browser.act` / `browser.goal` tools. Local
(Electron) tabs are unchanged and stay separate: a Cloud tab reference is
`{runtimeId: <profile id>, profileId: "cloud", tabId: <slot>}`.

## Pieces

| Part | Where | Role |
| --- | --- | --- |
| Worker + `ProfileBrowser` DO | `src/` | One DO per profile: acquires/reconnects the Browser Run session, streams frames to viewers, applies the person's input, runs one `BrowserHostAgent` per standing grant (the desktop host/goal code, imported, not copied), writes semantic checkpoints. |
| API | `apps/api/internal/cloudbrowser`, migration 0068 | Profiles, incarnations, grants (rows in `browser_tab_attachments`), sealed checkpoints, sealed optional Jev key, viewer tickets, wakes. |
| Web | `apps/web/src/browser/`, route `/browser` | Viewer, tab strip, address bar, share popover, control pill (「あなたが操作中」/「秘書が操作できます」), recovery and limits text. |
| Edge | `apps/web/cloudflare/worker.ts` | `/browser-cloud/viewer` WebSocket upgrades only, forwarded to the `SUMI_BROWSER_CLOUD` service binding without cookies or credentials. `/api/cloud-browser-host/*` is never reachable through the web edge. |

## Configuration (names only)

API (`cmd/server`): `SUMI_BROWSER_CLOUD_URL` (Worker origin, https), `SUMI_BROWSER_CLOUD_TOKEN`
(shared runtime secret, ≥32 chars), `SUMI_MODEL_CONNECTION_KEY` (existing; the
checkpoint and Jev-key sealing keys are derived from it). Without them the
person's overview answers `configured: false` and the rail item stays hidden.

Worker (`wrangler.jsonc`): `browser` binding `BROWSER`, DO `PROFILE`
(`ProfileBrowser`, sqlite), secret `SUMI_BROWSER_CLOUD_TOKEN`, vars
`SUMI_STATE_URL` (API origin; `env.alpha` reaches it through the VPC service
`SUMI_STATE`), `SUMI_BROWSER_IDLE_GRACE_MS` (default 60000),
`SUMI_BROWSER_KEEPALIVE_MS` (default 60000), optional `SUMI_JEV_ENDPOINT`
(default `https://api.typesafe.ai`).

Web (`apps/web/wrangler.jsonc`): service binding `SUMI_BROWSER_CLOUD` →
`sumi-browser-alpha`. Dev: `SUMI_DEV_BROWSER_CLOUD_ORIGIN` makes Vite proxy the
viewer socket.

Deploying the product Worker and the web binding is the coordinator's step;
nothing here was deployed to production.

## Lifecycle and cost

- A connected viewer or a claimed browser job keeps the browser alive. With
  neither, the DO checkpoints and closes the session after the idle grace.
  Hidden pages release the viewer after 5 minutes.
- The API wakes a sleeping profile only for queued work or undelivered
  grant/key changes; idle profiles get no polling or heartbeats.
- Durable writes: checkpoints (on load/idle, at most every 60 s while dirty),
  the slot map when tabs change, and an intent record written before and
  cleared after each dispatched action. No per-frame or per-heartbeat writes.
- Browser Run limits (concurrency, daily browser time on Free) apply; a start
  refused by Browser Run is shown as such and not retried in a loop.

## What survives a restart

Saved (sealed, ≤2 MiB per profile, bound to profile, owner, incarnation and
serializer version): cookies, `localStorage` strings, IndexedDB databases whose
records survive JSON (others are counted as not saved and reported), open tabs
with URL and scroll position, and grants (by stable tab slot).

Not saved: page memory, form drafts, `sessionStorage`, history, HTTP cache,
service workers, Cache Storage, OPFS. The UI says so (「保存される内容と使えない機能」).

A recreated browser is a new incarnation: earlier observations, tickets and
in-flight work are refused (`stale_observation`); an action whose effect was
uncertain when the browser was lost is reported as unknown and never replayed.

## Control

The person's input (except bare mouse moves) takes control before it is
dispatched; the secretary's queued actions are then refused with
`page_changed` and running Jev goals end `stopped_by_person`. 「秘書に戻す」
hands control back. Admitted input that already reached the page finishes and
is reported. Japanese IME: composition happens in the person's browser; the
committed text is inserted remotely.

Not supported (listed in the UI): file upload including drag and drop (a drop
shows a notice), downloads, copying from the remote page to the person's
clipboard (pasting into it works), audio, extensions. Live View is not used.

## Tests

- `pnpm test` here: input mapping, tickets, tab port.
- `apps/api`: `go test ./internal/cloudbrowser/ ./internal/browsertabs/`.
- Journey (`apps/web/e2e/cloud-browser.spec.ts`), real API + Core secretary +
  Web UI + this Worker:
  - local: `CLOUD_BROWSER_E2E_DB_URL=… npx playwright test e2e/cloud-browser.spec.ts`
    (uses `scripts/local-pool.mjs`, a headless Chrome stand-in for Browser Run,
    and a loopback Jev double).
  - cloud: deploy `wrangler.test.jsonc` (isolated test Worker with synthetic
    fixture sites) with `--var SUMI_STATE_URL:<API origin reachable from
    Cloudflare>`, put `SUMI_BROWSER_CLOUD_TOKEN`, then run with
    `CLOUD_BROWSER_E2E_MODE=cloud CLOUD_BROWSER_E2E_WORKER_URL=… CLOUD_BROWSER_E2E_TOKEN_FILE=…`
    and optionally `CLOUD_BROWSER_E2E_JEV_KEY_FILE`. `GET /__fixture/limits`
    (bearer) reads Browser Run usage without using browser time. Delete the
    test Worker afterwards.
- `apps/web/e2e/support/cloud-browser-dev.ts`: keeps the local stack running for
  manual use (session cookie written to a 0600 file, never printed).
