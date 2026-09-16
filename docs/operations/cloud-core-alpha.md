# Secretary core on Cloudflare (alpha)

The alpha secretary core runs as the `sumi-core-alpha` Worker: one Durable
Object (`SecretaryObject`) per secretary. Canonical state stays in the Go API
and PostgreSQL on the WSL origin. The Durable Object keeps only its persona id
and alarm, so a process restart, deploy or eviction loses no conversation,
pending request or memory.

## Route

```text
person ─ browser ─ sumi-alpha Worker ─ VPC Service sumi-alpha-api ─ tunnel ─ API :8080 ─ PostgreSQL
                                                                              │
     Messaging attention → core_inputs (API) ── wake sweep, every 1s ─────────┘
                                                        │ POST /personas/:id/wake (bearer)
                                                        ▼
                        sumi-core-alpha Worker → SecretaryObject (Durable Object)
                                                        │ SUMI_STATE: the same VPC Service
                                                        ▼
                        API /internal/core/* (runtime credential) → PostgreSQL
                        replies: messaging.send effect → Messaging → WebSocket
```

- The API sweep wakes a secretary when it has a queued input, an input left
  claimed by a writer whose lease expired, or a due schedule, and no live
  writer lease. A wake that does not land is found again on the next sweep;
  unchanged work is re-woken with a gap doubling from 5s to 5min.
- A wake's 200 means the Durable Object started its writer (lease +
  recovery) and armed its alarm — not that work finished. A startup
  failure, including the Worker's runtime secret being rejected, answers
  503 and appears in the API log as `not woken (will retry): core host
  answered 503`.
- While a drain runs, the Durable Object holds the writer lease and takes new
  inputs itself. An active secretary also has a 30s alarm, which continues
  memory preparation and schedules without any wake.
- A secretary without a selected model connection gets a visible
  `turn_failed` ("no model connection is selected …"). No operator model
  answers for it. ChatGPT selections are not implemented by this core and fail
  the same way.

## Credentials

| Name | Where | Purpose |
| --- | --- | --- |
| `SUMI_CORE_STATE_TOKEN` | API env | Admin credential for core state. Mounts `/internal/core`. It is not given to the Worker. |
| `SUMI_CORE_RUNTIME_TOKEN` | API env and Worker secret (same value) | Accesses any persona's scoped state routes. It cannot create personas, bind humans, decide approvals or transfer. |
| `SUMI_CORE_WAKE_URL` | API env | `https://sumi-core-alpha.<workers.dev subdomain>.workers.dev` |
| `SUMI_CORE_WAKE_TOKEN` | API env and Worker secret (same value) | Bearer for `/personas/:id/wake`, `/personas/:id/check` and `/health/state` |
| `SUMI_TRANSFER_PUBLIC_BASE_URL` | API env | Optional. The API's own trusted public origin (`https://api.<domain>`). Mounts `/api/secretary-transfer` and offers "bring my Local secretary" to new registrants. Never derived from the request Host. Unset means the choice is not offered and registration provisions a new secretary directly. |

Tokens must be at least 32 characters. The runtime token must differ from the
state token. Keep them in files with `0600` permissions outside the repository.
Do not put them in `wrangler.jsonc`, logs or documents.

## Preconditions

- **Feature availability is not the constraint.** SQLite-backed Durable
  Objects are available on both Workers Free and Paid plans
  (https://developers.cloudflare.com/durable-objects/platform/pricing/),
  and Workers VPC is free on all Workers plans while in beta
  (https://developers.cloudflare.com/workers-vpc/configuration/vpc-services/).
- **Verify capacity on the account's current plan.** The docs do not
  settle every Free-plan limit for this workload. Check the actual
  invocation limits and measured usage before deciding whether more
  capacity is needed:
  - Per the Workers limits page
    (https://developers.cloudflare.com/workers/platform/limits/), a Free
    invocation is limited to 10ms CPU and 50 external subrequests
    (internal subrequests to bound Workers count separately, 1000); Paid
    raises CPU to 30s default, configurable to 5min. Waiting on network
    I/O does not consume CPU time.
  - The Durable Objects limits page
    (https://developers.cloudflare.com/durable-objects/platform/limits/)
    lists "CPU per request: 30 seconds (default) / configurable to 5
    minutes" without distinguishing plan, while its FAQ says Durable
    Objects share the plan's per-invocation CPU limits — whether a
    Free-plan DO invocation gets 10ms or the 30s DO default is not
    settled by the docs.
  - This workload: a turn makes about a dozen state calls (well under
    the 50-subrequest cap), each small JSON over the binding; a turn or
    alarm drain is I/O-wait dominated, and the 14-minute drain is wall
    time inside the alarm handler's documented 15-minute wall limit.
    The binding constraint on Free would be DO CPU per invocation and
    daily DO request/duration quotas (100k requests, 13k GB-s/day) —
    real usage is measurable only on the deployed account.
  - `wrangler deploy --dry-run` reporting `default_usage_model=standard`
    is a config-level check, not subscription entitlement: confirm the
    plan on the account itself.
- The API image is built from a revision that contains this route (runtime
  credential and wake sweep).
- Setting `SUMI_CORE_STATE_TOKEN` on the API applies the core migrations. It
  also routes Messaging attention for every secretary on that API to the core
  instead of the previous agent runtime. That switch is the cutover. Removing
  the four variables and restarting the API reverses it. Core tables remain.

## Apply

Run from `apps/core` with Node 22 and the Wrangler login for the Sumi account:

```sh
umask 077; d=~/.local/share/sumi-alpha-core; mkdir -p "$d"
for f in state-token runtime-token wake-token; do [ -s "$d/$f" ] || openssl rand -hex 32 > "$d/$f"; done

node node_modules/wrangler/bin/wrangler.js deploy --env alpha --dry-run   # expect SECRETARY, SUMI_STATE (VPC Service), vars
node node_modules/wrangler/bin/wrangler.js deploy --env alpha
node node_modules/wrangler/bin/wrangler.js secret put SUMI_CORE_RUNTIME_TOKEN --env alpha < "$d/runtime-token"
node node_modules/wrangler/bin/wrangler.js secret put SUMI_CORE_WAKE_TOKEN --env alpha < "$d/wake-token"

# Before the API release: only liveness is checkable — no persona exists
# yet, so the readiness probe cannot run. curl /health is enough here.
curl -s "$CORE_URL/health"
```

Next, release the API with the four environment variables from the files
above, using the normal API release procedure. Then run:

```sh
node ../../scripts/operations/cloud-core-probe.mjs --core "$CORE_URL" --wake-token-file "$d/wake-token" \
  --state http://100.116.25.99:8080 --runtime-token-file "$d/runtime-token" --persona "$PERSONA_ID"
```

`--persona` is required — without it the script exits before printing a
result, so a probe that never reached a Durable Object can never print
PASS. It must name a persona that already exists in core state (one is
created when Messaging attention first lands for an agent, or via
`POST /internal/core/personas` with the state token); the probe never
picks or creates one itself. The `do-runtime-auth`
check is the essential one here: inside the Durable Object it reads that
persona's state through the `SUMI_STATE` binding with the Worker's
*installed* `SUMI_CORE_RUNTIME_TOKEN`. A wrong secret there — a file
mix-up, a one-sided rotation — is exactly the failure that leaves every
input queued while `health`/`state-path` still pass, and this probe now
fails on it instead of passing. `wake-persona` then confirms a wake starts
the runtime (503 would mean the DO could not start a writer).

The API log shows `core wake: sweeping for personas awaiting a runtime`. The
probe does not send a message.

## Verify with real use

These checks need the deployed environment. Local workerd does not replace
them.

1. Sign in to the alpha site. Send a message in a conversation with a
   secretary whose human selected an API connection. A reply arrives, and
   `wrangler tail --env alpha` shows `model bound to selected connection`.
2. Interruptions:
   - Redeploy the Worker (`wrangler deploy --env alpha`) during an idle
     period, then send a message. The reply arrives without manual action.
   - Restart the API during a reply. The same message completes once after
     the API returns.
3. Approvals: ask for an action that needs approval. It appears in the
   approval inbox. After approval, it runs once without a new message.
4. A secretary whose human has no selection gets a visible failure, not a
   reply.

## Operating bounds

What the sweep's fixed bounds mean in practice:

- Each sweep wakes at most 8 personas concurrently with a 10s HTTP timeout
  and runs to completion before the next interval starts. If many wakes
  hang, one sweep can take ~`ceil(awaiting/8) * 10s` (250s for a full
  200-row batch of dead hosts) and work admitted meanwhile waits for it.
  At alpha scale this is a tail case, not a steady state.
- A persona whose pending work does not change is re-woken with a gap
  doubling from 5s to a 5min ceiling. After a Worker outage, a cold
  secretary (no alarm yet) can therefore wait up to ~5 minutes for its next
  wake once the gap has grown; a secretary that was already running keeps
  its 30s alarm and is not affected.
- Candidate selection is bounded (200 rows per sweep) and rotates: each
  sweep continues after the previous page's last persona id, wrapping at
  the end of the set. Every awaiting persona is therefore *selected*
  within `ceil(awaiting/200)` sweeps no matter how long batches take —
  application backoff can delay a wake but can never hold the head of the
  queue and starve later personas. A persona is woken when selected and
  due: its re-wake gap elapsed, or its pending work changed.

## Platform assumptions verified against primary documentation

Checked against developers.cloudflare.com (2026-09-15):

- A Durable Object is kept alive by the incoming request/event it is
  processing — an alarm handler invocation has a documented 15-minute wall
  time (limits page), which is why memory preparation runs only inside
  alarm drains (`SUMI_ALARM_DRAIN_LIFETIME_MS`, 14min). A **plain outgoing
  `fetch()` never prevents eviction** of an idle object, and
  `ctx.waitUntil` does not extend a DO's lifetime; only outbound
  TCP/WebSocket connections defer eviction, and only up to 15 minutes. A
  non-hibernating idle object is evicted after 70–140s of inactivity. The
  fetch-started drain's 25s turn budget stays under that floor; correctness
  never depends on an outgoing call holding the object.
- `vpc_services` is in beta and free on all Workers plans
  (https://developers.cloudflare.com/workers-vpc/configuration/vpc-services/);
  the configuration pages do not state whether a VPC Service binding is
  usable **from inside a Durable Object**. The `do-runtime-auth` probe check
  exercises exactly that path on the deployed Worker — until it has run
  against the real deployment, this remains an assumption, not a verified
  fact.
- Every state call carries a 10-second `AbortSignal` deadline
  (`STATE_CALL_TIMEOUT_MS` in `apps/core/src/state-client.ts`) covering
  the request and the response body read. A call that never answers, or
  answers headers and stalls mid-body, fails as a transient 503: the
  serialized start frees so the next wake or alarm can recover the same
  input, and the caller's normal retry under server-side idempotency and
  generation fencing decides whether the mutation landed. Verified in
  local workerd through the `SUMI_STATE` binding (e2e S8): the hung
  subrequest rejected at the deadline and the queue freed without a
  restart. The stand-in's upstream abort listener and body cancellation
  callback did not fire; releasing the caller did not establish that the
  service handler stopped. Whether the *deployed* VPC binding propagates cancellation the
  same way — and whether a real VPC blackhole is rejected at the deadline —
  is a live check. Local workerd additionally cancels requests it can
  prove can never complete ("would never generate a response"), so the e2e
  stand-in keeps a timer pending to exercise the deadline path itself.
- `wrangler deploy --dry-run` reporting `default_usage_model=standard` is
  a config-level check, not subscription entitlement: confirm the Workers
  plan and usable capacity on the account separately.

Still requiring the live deployment (local workerd cannot prove them):
VPC Service behavior inside a DO (including cancellation/timeout on a
blackholed service), tunnel failure shapes, DO eviction and
alarm continuity across deploys, and turns longer than the fetch drain's
25s budget riding the alarm's 15-minute window.

## Local check

`apps/core/scripts/e2e-workerd-cloud.mjs` runs the `alpha` environment's
bindings and variables in local workerd against a real Go state service and
PostgreSQL. It swaps the VPC Service for a loopback stand-in and uses a
scripted model. It checks conversation, a workerd kill mid-turn, workerd
being down when a message is admitted, a state-service kill mid-turn, a
secretary with no selected model, approval resume, a wrong installed
runtime secret, and a state call that wedges (before headers and mid-body)
timing out at the deadline and recovering through a later wake:

```sh
SUMI_TEST_DB_URL=postgres://… SUMI_E2E_PORT_BASE=11871 node apps/core/scripts/e2e-workerd-cloud.mjs
```
