# Decision inbox

A small, private Cloudflare Workers PWA for the period before Sumi can deliver its own notification intents. Codex or Claude can publish a bounded decision request; one Human can open the exact request on a phone, answer it, and let the publisher poll the durable result.

This is deliberately a separate product boundary. It does not import Sumi runtime packages, call Sumi domain APIs, or introduce its temporary authentication model into Sumi. When Sumi messaging and notification intent are ready, replace this app instead of teaching it multi-user or Workspace concepts.

## Product contract

- Publisher API: bearer-authenticated create, poll, unresolved list, cancel, and one-time Human link minting.
- Human PWA: one-time bootstrap exchange, HttpOnly session, pending/history/detail views, two-tap choice flow, optional short reply, contextual push setup, and clear terminal states.
- Durable state: D1 is authoritative. A cached shell and last successful reads remain visible offline, but writes are disabled and never queued or reported as successful.
- Delivery: standard Web Push with VAPID. Push opens `/requests/:id`; 404/410 subscriptions are deleted. At most four recently confirmed phone/browser subscriptions are retained and contacted per decision. Optional callbacks are one-shot, signed hints. Publisher polling remains authoritative.
- Scope: one Human, one publisher credential, multiple phone/browser subscriptions. No org, Workspace, role, or multi-user model.

## Architecture

`src/worker.ts` serves the native Worker API and built Vite assets. D1 stores request state, exactly one response per request, one-time bootstrap hashes, session hashes, bounded subscriptions, immutable callback delivery identity, and small fixed-window rate counters. The browser never receives the publisher token or VAPID private key.

The important state transitions are:

```text
pending ── Human response ──> resolved
   │
   ├── publisher cancel ────> cancelled
   └── expiry-on-read ──────> expired
```

Only `pending` may transition. Response submission uses a request-local idempotency key. Replaying the same key returns the recorded response; a different attempt after resolution returns `409`.

## Local setup

Requirements: Node 22+, pnpm, and a Chromium browser for the optional Playwright smoke. The smoke uses `/usr/bin/google-chrome` by default; set `PLAYWRIGHT_CHROME_PATH` when Chrome lives elsewhere.

```bash
pnpm install
cd apps/decision-inbox
cp .dev.vars.example .dev.vars
pnpm exec web-push generate-vapid-keys
```

Put the generated VAPID pair in `.dev.vars`. The example publisher, bootstrap, and signing values are only for isolated local use.

Apply the local database migration, build the web app, and run the full Worker:

```bash
pnpm d1:local
pnpm build:web
pnpm dev:worker
```

Open `http://localhost:8787/#bootstrap=<HUMAN_BOOTSTRAP_SECRET>`. The fragment is removed before the app continues. A configured bootstrap value is one-time; mint another link through the publisher endpoint for another device.

For frontend-only iteration, run `pnpm dev` while the Worker runs on port 8787. Vite proxies `/api`.

## Publisher examples

Create a request. The same `Idempotency-Key` and byte-equivalent parsed payload return the same request. Reusing the key for different content returns `409`.

```bash
curl --request POST http://localhost:8787/api/publisher/requests \
  --header 'Authorization: Bearer local-publisher-token-change-me' \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: cutover-2026-08-10-01' \
  --data '{
    "title": "Choose the cutover window",
    "body": "The checked release head is ready. Which window should Codex use?",
    "source": "Codex · Workspace cutover",
    "choices": [
      {"id":"now","label":"Proceed now","tone":"positive"},
      {"id":"later","label":"Wait until morning","tone":"neutral"},
      {"id":"stop","label":"Stop the change","tone":"destructive"}
    ],
    "allowFreeText": true,
    "expiresAt": "2026-08-10T23:30:00+09:00"
  }'
```

The response contains `request.id`, an authenticated `statusUrl`, and a Human `humanUrl`.

Poll one request or list unresolved requests:

```bash
curl --header 'Authorization: Bearer local-publisher-token-change-me' \
  http://localhost:8787/api/publisher/requests/REQUEST_ID

curl --header 'Authorization: Bearer local-publisher-token-change-me' \
  http://localhost:8787/api/publisher/requests
```

Cancel an unresolved request:

```bash
curl --request POST \
  --header 'Authorization: Bearer local-publisher-token-change-me' \
  http://localhost:8787/api/publisher/requests/REQUEST_ID/cancel
```

Mint a one-time phone link:

```bash
curl --request POST http://localhost:8787/api/publisher/bootstrap-tokens \
  --header 'Authorization: Bearer local-publisher-token-change-me' \
  --header 'Content-Type: application/json' \
  --data '{"expiresInSeconds":3600}'
```

Callback routing is deployment-owned. Set one exact public HTTPS `CALLBACK_URL`; a request can then opt into delivery with `"callback":{"correlationId":"cutover-42"}`. A request may include `callback.url` only as an assertion, and it must exactly match `CALLBACK_URL`. It never selects an arbitrary destination. If callback is requested while the deployment has no valid destination, creation fails closed.

When the request resolves or is cancelled, the terminal D1 transition also mints and stores one immutable delivery ID and creation time. The Worker sends one best-effort `POST` whose exact JSON body contains `schema`, `delivery.id`, `delivery.createdAt`, `type`, and the request. The exact bytes are signed as `X-Sumi-Decision-Signature: sha256=<base64url HMAC-SHA256>` with `CALLBACK_SIGNING_SECRET`; the ID is also copied to `X-Sumi-Decision-Delivery-Id`.

**Receiver contract:** verify the signature over the raw body, then atomically claim/deduplicate `delivery.id` in the receiver's durable store **before** performing any effect. If the claim already exists, return success without repeating effects. The same delivery may be replayed with byte-identical body and signature even though this temporary sender has no retry queue. Publisher polling remains authoritative.

## Validation

```bash
pnpm check-types
pnpm test
pnpm build
pnpm test:e2e
```

`pnpm test` runs the API against the Cloudflare Workers Vitest pool and an actual local D1 binding. `pnpm build` includes a Wrangler dry-run bundle, which verifies that `web-push` bundles under the required `nodejs_compat` flag. The Playwright test starts a local Worker, uses a Pixel 7 viewport, creates a request through the publisher API, signs in through the one-time fragment, resolves the request, and saves screenshots under `test-results/`.

## Cloudflare deployment inputs

1. Create D1 and replace the placeholder `database_id` in `wrangler.jsonc`:

   ```bash
   pnpm exec wrangler d1 create sumi-decision-inbox
   ```

2. Generate VAPID keys once:

   ```bash
   pnpm exec web-push generate-vapid-keys
   ```

3. Set every live deployment value through Worker secret bindings. Use independent random credentials and do not reuse Sumi credentials. `CALLBACK_URL` and the VAPID public metadata are not confidential, but keeping every deployment-specific value out of source makes this temporary app easier to replace:

   ```bash
   pnpm exec wrangler secret put PUBLISHER_TOKEN
   pnpm exec wrangler secret put HUMAN_BOOTSTRAP_SECRET
   pnpm exec wrangler secret put SESSION_SIGNING_SECRET
   pnpm exec wrangler secret put CALLBACK_SIGNING_SECRET
   pnpm exec wrangler secret put CALLBACK_URL
   pnpm exec wrangler secret put VAPID_PUBLIC_KEY
   pnpm exec wrangler secret put VAPID_PRIVATE_KEY
   pnpm exec wrangler secret put VAPID_SUBJECT
   ```

4. Apply migrations, perform a dry run, then deploy intentionally:

   ```bash
   pnpm exec wrangler d1 migrations apply sumi-decision-inbox --remote
   pnpm deploy:dry-run
   pnpm deploy
   ```

Keep `COOKIE_SECURE=true` in production. The live Worker should use a custom HTTPS hostname before phone onboarding.

## Security and replacement boundary

- Human writes require the Strict same-site session cookie, exact same-origin `Origin`, and a derived CSRF token.
- Raw bootstrap and session tokens are never stored. Publisher auth is a Worker secret; rows carry only a hash of the stable, non-secret `PUBLISHER_ID`, so the bearer secret can rotate without orphaning requests. Push endpoints must remain reversible because they are delivery addresses.
- Request title/body/reply are bounded plain text. The PWA renders React text nodes and has no Markdown or HTML renderer.
- Callback requests can target only the deployment's exact public HTTPS `CALLBACK_URL`. Callback delivery follows no redirects, and receivers must atomically deduplicate the signed immutable delivery ID before effects.
- Push retention is deliberately small: at most four active subscriptions, idle entries are removed after 45 days, and records are renewed or removed after an absolute 180 days. The browser refreshes `last_seen_at` by re-registering its existing subscription when the app opens. Stale/aged pruning, upsert, and cap enforcement share one atomic D1 batch; delivery independently limits fan-out to four.
- Rate counters bound bootstrap, publish, read, response, and subscription routes for personal use. They are not an abuse-prevention product.
- Later authentication should be Cloudflare Access or Sumi Human auth. Later delivery should be Sumi notification intent/outbox. Do not grow this bootstrap/session scheme into shared product infrastructure.

## Known limits

- Web Push delivery depends on the phone/browser push service and is not an execution guarantee.
- Callback delivery is a single best-effort attempt; polling is the source of truth.
- Only four recently confirmed push subscriptions are retained; opening an older device lets its existing authorized subscription reclaim an active slot.
- Lists are capped at 100 rows with no pagination.
- There is no admin UI, credential rotation UI, multi-Human isolation, or data export.
- iOS requires installing the PWA to the Home Screen before standards-based Web Push is available.

**Deployment state:** no live Cloudflare resources were created and no live deployment was performed by this implementation.
