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
- The API wakes a profile only for queued work or undelivered grant/key
  changes; idle profiles get no polling or heartbeats. A refresh wake that the
  Worker answers without a running browser (`phase` other than
  live/starting/restoring/saving) clears the pending refresh: the next start
  receives everything. Such a wake arms no DO alarm.
- Starting asks Browser Run first, then the API (`begin`, new incarnation). A
  start refused by Browser Run changes nothing in the API. A start failing
  after `begin` closes the acquired session and reports the incarnation
  `sleeping`. Either way queued work retries only after a backoff (30 s,
  doubling to 15 min; the Worker answers `retry_after_ms` and the API holds
  its work wakes that long). The person's 「再開」 retries at once.
- A session is reconnected to after a host restart only if its start
  finished (`meta.ready`); a half-started or half-restored one is closed.
  Reconnecting continues the stored checkpoint sequence and carried origins.
- Durable writes: checkpoints (on load/idle, at most every 60 s while dirty;
  a failed save is retried after 5 s, doubling to 5 min), the slot map when
  tabs change, and an intent record written before and cleared after each
  dispatched action. No per-frame or per-heartbeat writes.

## What survives a restart

Saved (sealed, ≤2 MiB per profile, bound to profile, owner, incarnation and
serializer version): cookies, `localStorage` strings, IndexedDB records whose
key and value are plain JSON data, open tabs with URL and scroll position, and
grants (by stable tab slot).

- IndexedDB records holding anything else (Blob, File, Date, ArrayBuffer and
  typed arrays, Map/Set, binary or Date keys) are skipped and counted, never
  converted.
- Cookies and tabs are always saved. Each origin's storage is added while it
  fits the budget (the active tab's origin first, then other open tabs, then
  origins carried from the previous checkpoint); one that does not fit is left
  out whole and listed (`notSaved.oversizedOrigins`), as is one that could not
  be read (`skippedOrigins`). The page script stops reading at the remaining
  budget.
- Restoring skips a record the new browser refuses and an origin whose
  restore fails; the rest of the profile is restored and the person is told
  which sites did not come back.
- The UI shows, from the latest checkpoint, what was left out (footer and
  「保存される内容と使えない機能」).

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
is reported.

Input names the tab of the frame the person saw; if the active tab changed
meanwhile (the secretary switched the screen), it is refused with a
`tab_changed` notice instead of landing on another page.

A page reload or a brief disconnect keeps the person's control: with no viewer
connected, control stays with the person for 2 minutes (`HUMAN_HOLD_MS`) and
the browser stays up. A viewer back within that time sees 「あなたが操作中」
unchanged. After it, control returns to the secretary, the next viewer is told
so (「接続が切れたまま 2 分たったため…秘書に戻しました」), and the idle grace
applies. A goal stopped by the takeover is never restarted. Tradeoff: during
the hold the secretary's actions are refused (`page_changed`) even though
nobody is watching; 「秘書に戻す」 remains the ordinary way to hand back, and
the viewer then says so (「…『秘書に戻す』で操作を秘書に戻しました」).

The takeover belongs to the profile and survives a Durable Object restart or
Worker deploy. It is written to DO storage (`control`: mode, epoch, hold
deadline, last return reason) only at transitions — takeover, hold start, hold
cleared by a returning viewer, return to the secretary — never per input,
frame or poll. The person's input that takes control is sent to the page only
after that write; the secretary is fenced in memory at once. On restart:

- viewers that were connected (their close may never have run) count as
  disconnected: the hold starts at restore, so the secretary may wait up to
  2 minutes from whenever the object next wakes;
- a hold in progress keeps its original deadline, and one already past is
  returned to the secretary (`viewer_absent`) before any secretary tick;
- if the live browser is gone and a fresh one starts, the person keeps
  control under the same hold. Stopped goals stay stopped.

If the write fails, control stays with the person, viewers get a
`control_not_saved` notice, and the alarm loop retries every 5 s; a restart
before it lands loses the takeover. Sleep keeps the mode (new epoch); deleting
the browser clears it.

A Jev key saved or deleted while a tab's agent is mid-tick (polling or running
a goal) reaches that agent when the tick ends. A key Jev refuses is reported
with its version (`jev_key_version`), and the API marks only that version
rejected, so a key saved since is never withdrawn by an older key's failure. Japanese IME: composition happens in the person's browser; the
committed text is inserted remotely.

Not supported (listed in the UI): file upload including drag and drop (a drop
shows a notice), downloads, copying from the remote page to the person's
clipboard (pasting into it works), audio, extensions. Live View is not used.

## Tests

- `pnpm test` here: input mapping, tickets, tab port, the ProfileBrowser
  lifecycle (`test/profile.test.ts`: reconnect/checkpoint continuity,
  refused and failed starts, restore failure, save backoff, Jev key rotation,
  input tab, control hold), control persistence across restarts
  (`test/control-persistence.test.ts`), checkpoint budget and restore isolation, and the
  checkpoint page scripts in real headless Chrome
  (`test/storage-scripts.chrome.test.ts`; skipped without google-chrome).
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
