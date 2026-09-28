# ChatGPT subscription connections

A person can connect their own ChatGPT plan as a model connection ("Sign in
with ChatGPT"). Only the model call uses the subscription. Sumi's context,
tools, memory and agent loop stay the same. This is not a Codex agent and does
not use the Codex app-server.

The feature is **off by default**. Deciding whether a deployment may enable it
is a policy question, not a technical one; see [Policy status](#policy-status).

## Enable

The main Go API requires all of the following:

- PostgreSQL with migration `0002_chatgpt_subscription` applied. `db.Migrate`
  applies it on start.
- `SUMI_MODEL_CONNECTION_KEY`, the 32-byte base64 key that already seals API
  keys.
- `SUMI_CHATGPT_SUBSCRIPTION=enabled`. Any other non-empty value except
  `disabled` stops the server from starting. Setting `enabled` without the
  key also stops it from starting.

With the gate off, the connection list reports
`chatgpt: {available: false}`, the login routes answer 409, and existing
subscription connections stop working. They fail visibly and are never
replaced by another model.

The account-free Local host (`sumi-local`) has no sign-in path for this yet.

## Sign-in flow (device code)

1. In "AIの接続", the person chooses "ChatGPTで接続". The API starts a
   device-code login with OpenAI's issuer. The browser shows
   `https://auth.openai.com/codex/device` and a short code.
2. The person opens that page, signs in to ChatGPT there, and enters the
   code. The panel first reminds them to enable device-code login in their
   ChatGPT security settings (for a workspace account, an admin allows it in
   the workspace permissions); OpenAI's Codex docs list that as the first
   step. A 404 when starting means ChatGPT's side does not currently offer
   device-code login (the Codex client reads it the same way); the
   per-account setting is not known when the code is requested.
3. The browser reads `GET /api/model-connections/chatgpt/login/{id}` at the
   interval the issuer asked for. The API polls the issuer only on those
   reads, under a row lock, so several tabs or API processes exchange the
   code once.
4. On success, the API stores the grant sealed. A new connection is also
   selected; a reconnect repairs that connection and leaves the person's
   selection as it was. New connections default to `gpt-6-astra` with
   reasoning effort `medium`.

Tokens never pass through the browser. A pending login is tied to the
person and the browser session that started it, and expires after 15
minutes. Only the explicit "キャンセル" cancels it: closing the panel, a
reload or a discarded mobile tab leaves it pending, and starting again in
the same browser session for the same target returns that login and its
code (while at least a minute is left) instead of replacing it. A start
from another session, or for another target, cancels the person's older
pending login.

Once the issuer may have consumed the one-time code, the exchange and the
save run on their own bounded context (45 seconds), so a browser that goes
away mid-exchange does not lose the sign-in. Starting, polling and
cancelling take their locks in one order (login rows, then the person, then
the connection), so a start or cancel during a completing poll waits for it
instead of deadlocking; a cancel that arrives after completion reports
`completed`.

| Method | Path | Result |
|---|---|---|
| POST | `/api/model-connections/chatgpt/login` | Start. Body `{connectionId?}`; with an id, a completed login reconnects that connection |
| GET | `/api/model-connections/chatgpt/login/{id}` | Status: `pending`, `completed`, `failed`, `expired` or `cancelled` |
| DELETE | `/api/model-connections/chatgpt/login/{id}` | Cancel |
| PUT | `/api/model-connections/chatgpt/{id}` | `{name?, model, reasoningEffort}` (`low`, `medium`, `high`, `xhigh` or `max`; the settings form offers the values each suggested model lists and always keeps the stored one) |

The API-key routes cannot edit or resolve a subscription connection.

## Tokens and refresh

- The grant (access, refresh and ID token plus the account id) is sealed
  with AES-GCM under `SUMI_MODEL_CONNECTION_KEY`. The authenticated data is
  bound to the person, the connection and the account.
- The Core reads the person's model binding for every call. For a
  subscription connection, the binding carries a short-lived access token and
  the account id. The state service refreshes the token while holding the
  connection's row lock, so concurrent turns and API processes refresh once.
  It refreshes when the token is within 5 minutes of expiry. A rotated
  refresh token is stored before the new access token is handed out.
- The refresh and the write of its result run on their own bounded context
  (30 seconds), not the caller's. The issuer rotates the refresh token as
  soon as it answers, so a Core timeout, a cancelled Worker request or a
  dropped connection must not discard the answer: the next call finds the
  rotated grant stored. The Core waits up to 35 seconds for the two
  model-credential calls (other state calls keep 10 seconds), so an
  ordinary slow refresh finishes within the turn.
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
`reasoning.context: "all_turns"`. The backend then returns each reasoning
item with opaque encrypted content. The Core keeps, per round, those
reasoning items (encrypted content and item id only; any readable summary
or reasoning text is dropped) and the ids and order of the round's message
and function-call items. This is stored with the round in the durable plan
(`core_turn_plans`, field `continuation`), and the next round's request
replays it in the original order, byte for byte. Because it lives in the
plan, a retried or resumed attempt of the same turn sends the same bytes the
live attempt would have.

- Scope: continuation is sent only to the same account and model (a digest
  of both is stored with it). After a switch it is not sent.
- Recovery: if the backend answers 400 to a request carrying recorded
  continuation, the Core resends once without it, so unusable stored bytes
  cannot wedge a turn.
- Size: one round's continuation is kept only up to 1 MiB and only when the
  plan still fits its request budget; otherwise the round is recorded
  without it.
- Limits: continuity is within one turn's tool rounds. Earlier turns are
  rendered from the journal, which holds no reasoning, so reasoning does not
  carry across turns. The API-key Responses wire (`openai-responses`) is
  unchanged and never sends continuation.

The protocol follows the public `openai/codex` client source. It has been
tested only against synthetic fixtures. The following are still unproven
against the live backend:

- whether it accepts `originator: sumi`;
- whether it requires client attestation;
- whether it accepts replayed reasoning items and function-call item ids
  across tool rounds as sent here;
- whether Cloudflare Workers egress to `chatgpt.com` is challenged.

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
