# ChatGPT subscription connections

A person can connect their own ChatGPT plan as a model connection ("Sign in
with ChatGPT"). Only the model call uses the subscription. Sumi's context,
tools, memory and agent loop stay the same. This is not a Codex agent and does
not use the Codex app-server.

The feature is **off by default**. Once enabled, it is available to every
signed-in person under the existing authentication and ownership boundaries.
There is no additional role or individual allowlist. The current release is
for the developers already admitted to this deployment; wider hosted use
remains an open question (see [Policy status](#policy-status)).

## Enable

The main Go API requires all of the following:

- PostgreSQL with migration `0002_chatgpt_subscription` applied. `db.Migrate`
  applies it on start.
- `SUMI_MODEL_CONNECTION_KEY`, the 32-byte base64 key that already seals API
  keys. Keep the existing key; replacing it makes existing sealed credentials
  unreadable.
- `SUMI_CHATGPT_SUBSCRIPTION=enabled`. Any other non-empty value except
  `disabled` stops the server from starting. Setting `enabled` without the
  key also stops it from starting.

With the gate off, the connection list reports
`chatgpt: {available: false}`, the login routes answer 409, and existing
subscription connections stop working. They fail visibly and are never
replaced by another model.

The account-free Local host (`sumi-local`) has no sign-in path for this yet.

## Sign-in flow (device code)

1. In "AIの接続", the person chooses "ChatGPTで接続". Before issuing a
   code, the panel explains Codex device authentication, Sumi's server-side
   encrypted storage, and use of the person's Codex quota. It explains that
   the provider page may say "Codex CLI" and that only a code the person
   just started in this Sumi setup should be approved.
2. "ログインコードを発行" starts the attempt. The browser shows
   `https://auth.openai.com/codex/device`, the code, expiry and progress.
   The panel explains the prerequisite: enable device-code login in the
   ChatGPT account's security settings; workspace accounts also require
   administrator permission. A 404 while issuing a code says only that
   device-code login is currently unavailable at the issuer. The server
   cannot diagnose the person's account setting at that point.
3. The browser reads `GET /api/model-connections/chatgpt/login/{id}` at the
   issuer's interval. The API polls only on these reads. Human, login and
   reconnect-target locks serialize replacement and code exchange across
   tabs and API processes. A partial unique index enforces one pending
   login per person.
4. On success, the API stores the grant sealed. A new connection is also
   selected; reconnecting repairs that connection and preserves the current
   selection. New connections default to `gpt-6-astra`, effort `medium`.

Tokens never pass through the browser. A pending login is bound to the
person, authenticated browser session and reconnect target; its code
expires after 15 minutes. The browser saves only a random attempt UUID and
whether the person requested cancellation, in `sessionStorage`. It saves
that intent before sending the request. Reopening the panel or reloading
the same tab resends the same ID and gets the same pending, completed or
terminal attempt. In particular, a lost completion response does not issue
a second code. After observing completion, an intentional additional
connection starts with a new ID. Terminal rows are retained until a later
start cleans rows older than 24 hours. A UUID cannot be rebound to another
person, session or target.

Closing the panel aborts only the browser request. Explicit "キャンセル"
is a durable request: the panel displays progress and checks the actual
server result, keeping cancellation intent for retry/reopen if the result
is lost. If authorization already completed, cancellation reports the
completed connection and does not delete its grant. A new attempt ID
replaces an older pending attempt only after acquiring the person's lock
and rechecking. Browser storage being disabled limits recovery to the
mounted panel; clearing tab storage or closing the tab loses its local
attempt handle. The grant and connection list remain server-side.

All login operations lock in the order **human → login → connection**.
The API bounds lock acquisition at 5 seconds, then gives a fresh issuer
budget (25 seconds for code issuance, 45 seconds for polling plus the
one-time exchange), then a fresh 5 seconds to save/commit. Each issuer HTTP
request has its own 20-second timeout. Reconnect locks are acquired before
consuming a code. Explicit cancellation allows 50 seconds to wait out an
in-flight poll, then 5 seconds to persist. Browser login requests wait up to
60 seconds. These phases detach from caller cancellation; lock waiting
cannot consume the issuer or persistence budget.

| Method | Path | Result |
|---|---|---|
| POST | `/api/model-connections/chatgpt/login` | Start/recover. Body `{loginId, connectionId?}`; required `loginId` is a browser-generated UUID, `connectionId` identifies a reconnect target |
| GET | `/api/model-connections/chatgpt/login/{id}` | Status: `pending`, `completed`, `failed`, `expired` or `cancelled` |
| DELETE | `/api/model-connections/chatgpt/login/{id}` | Cancel or recover its result; returns the login view, including an already-completed connection |
| PUT | `/api/model-connections/chatgpt/{id}` | `{name?, model, reasoningEffort}` (`low`, `medium`, `high`, `xhigh` or `max`; the settings form offers the values each suggested model lists and always keeps the stored one) |

The API-key routes cannot edit or resolve a subscription connection.

## Tokens and refresh

- The grant (access and refresh tokens) is sealed
  with AES-GCM under `SUMI_MODEL_CONNECTION_KEY`. The authenticated data is
  bound to the person, the connection and the account.
- The Core reads the person's model binding for every call. For a
  subscription connection, the binding carries a short-lived access token and
  the account id. The state service refreshes the token while holding the
  connection's row lock, so concurrent turns and API processes refresh once.
  It refreshes when the token is within 5 minutes of expiry. A rotated
  refresh token is stored before the new access token is handed out.
- Refresh has independent detached budgets: 5 seconds to acquire the
  connection lock, 25 seconds for the issuer, then 5 seconds for saving and
  commit. A contended caller that cannot acquire the lock leaves before
  contacting the issuer and can retry. The Core waits up to 40 seconds for
  its two model-credential endpoints (other state calls keep 10 seconds).
  A caller timeout or disconnect cannot discard a successfully returned
  rotated token merely by spending the save budget while waiting on a row.
  This is not a distributed transaction with the issuer: a lost issuer
  response after consumption, process crash or database failure can still
  require reconnection.
- When the backend answers 401, the Core reports the rejected token by its
  SHA-256 digest to
  `POST /internal/core/personas/{persona}/model/credential-refresh`. It then
  sends the identical request once more with the refreshed token. A second
  401 ends the turn with `model_auth_rejected`: the refresh worked, so
  reconnecting would only repeat it, and why ChatGPT refused is not known to
  Sumi. The connection is not marked `reconnect_required`. The recorded
  error keeps the response's `error.code`/`error.type` only when it has the
  shape of an error identifier (lowercase letter words joined by `_`);
  anything else is recorded as `unrecognized`. The body is never logged.
- A refresh failure the issuer marks permanent (expired, reused or
  invalidated refresh token, `invalid_grant`, 401), or a token for another
  account, marks that one connection `reconnect_required`. The turn fails
  with `model_reconnect_required`, and the settings screen offers
  "ChatGPTに再接続". Other people's connections are unaffected.
- A token refresh keeps the connection version. A reconnect creates a new
  version.
- With the gate off, a turn on a selected subscription connection fails with
  `model_connection_disabled` (a connection is selected; this server does not
  offer its kind), not `no_model_connection`.

## Model calls

The Core posts to `https://chatgpt.com/backend-api/codex/responses` with
`fetch` only. The same code runs under Node and workerd. The Core checks the
binding's endpoint against that exact base URL before an access token is
sent; any other endpoint fails the call before a request.

- Headers: `Authorization`, `ChatGPT-Account-ID`, `originator: sumi` and
  `session_id` (the persona).
- Body: `store: false`, `stream: true`, and no output-token bound.
- Models the Codex catalog marks "responses lite" (`gpt-6-*`, `gpt-5.6-*`,
  `gpt-daybreak-*`) use that request shape. They send the
  `x-openai-internal-codex-responses-lite` header, and the tools and
  instructions go in as developer items rather than top-level fields. The
  table is static, taken from `openai/codex` at commit 44fe510c.
- A 429 with `usage_limit_reached` or `usage_not_included` fails the turn
  with `model_usage_limit`. The reset time, when given, is only in the
  recorded (private) error text; the chat shows the fixed explanation.
  Other 429 responses are retried like any rate limit.

### Reasoning continuation across tool rounds

As the Codex client does, every request sends
`include: ["reasoning.encrypted_content"]`, and lite models also send
`reasoning.context: "all_turns"`. When the backend returns reasoning items, their encrypted content is
opaque to Sumi. The Core keeps, per round, those
reasoning items (encrypted content and item id only; any readable summary
or reasoning text is dropped), each message's own assistant text, and the
function-call ids. Output positions use `output_index`, including when done
events arrive out of order. This is stored with the round in the durable plan
(`core_turn_plans`, field `continuation`), and the next round's request
replays opaque content byte for byte in the original output order. Separate
assistant messages retain their own positions around reasoning and calls. Because it lives in the
plan, a retried or resumed attempt of the same turn sends the same bytes the
live attempt would have.

- Scope: continuation is sent only to the same account and model (a digest
  of both is stored with it). After a switch it is not sent.
- Recovery: if the backend answers 400 to a request carrying recorded
  continuation, the Core retries once without encrypted reasoning or output
  item ids, preserving message/call order. This handles an explicit rejected
  request, not ambiguous acceptance. A lost model response is not evidence
  that no quota was consumed; only a saved plan allows deterministic resume
  without consulting the model again for that recorded round.
- Size: one round's continuation is kept only up to 1 MiB and only when the
  plan still fits its request budget; otherwise the round is recorded
  without it.
- Limits: continuity is within one turn's tool rounds. Earlier turns are
  rendered from the journal, which holds no reasoning, so reasoning does not
  carry across turns. The API-key Responses wire (`openai-responses`) is
  unchanged and never sends continuation.

The protocol follows the public `openai/codex` client source. Repair tests
use synthetic fixtures only. Root reported a separate live source-level
probe on candidate `3816aed6bf3d1e4ba692b3f341553e0c25180e1f`: device approval,
sealed grant, account match, three HTTP 200 model responses with
`originator: sumi`, and two harmless tool rounds without API-key fallback.
That probe returned no encrypted reasoning. It did not exercise the Sumi
browser/API selection flow, live refresh, workerd or Cloudflare egress.
Encrypted reasoning replay and those runtime paths remain unproven live;
these repairs require acceptance on the final integrated candidate.

## Policy status

The auth and transport are technically documented for Codex clients: see
[Codex authentication](https://learn.chatgpt.com/docs/auth), which covers
device-code sign-in and automatic token refresh. Whether a third-party
client may use them is a separate question. The only public statements found
are posts by OpenAI's Tibo Sottiaux (@thsottiaux), not terms of service:

- [2026-05-23](https://x.com/thsottiaux/status/2058071172361998482):
  "Reminder you can use your ChatGPT account in a flourishing set of other
  tools."
- [2026-08-21](https://x.com/thsottiaux/status/2090675027670978569):
  "Converting a subscription into api traffic to then re-serve or share across
  many users is not something we support". The same post says use through
  "one of the many OSS clients (Pi, OpenCode, ...)" is "completely fine".
- [2026-09-08](https://x.com/thsottiaux/status/2097131394199896166), replying
  to the launch of Companion, a service that "uses your own ChatGPT
  subscription": "Sorry, but this is not an approved use of Sign in With
  ChatGPT. We support pure OSS clients and others we have partnerships with,
  but you need to reach out and talk to us for that."

A hosted, multi-user deployment such as Sumi Cloud resembles the case called
not approved. A person running the open-source Sumi themselves resembles the
OSS-client case. Both points are interpretations, not OpenAI rulings. The
operator of each deployment decides whether to set the gate.
