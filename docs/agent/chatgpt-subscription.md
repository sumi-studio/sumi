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
   code. If the account's ChatGPT security settings have device-code sign-in
   for Codex turned off, the start fails with guidance to enable it.
3. The browser reads `GET /api/model-connections/chatgpt/login/{id}` at the
   interval the issuer asked for. The API polls the issuer only on those
   reads, under a row lock, so several tabs or API processes exchange the
   code once.
4. On success, the API stores the grant sealed and selects the connection.
   New connections default to `gpt-6-astra` with reasoning effort `medium`.

Tokens never pass through the browser. A pending login is tied to the
person and the browser session that started it. It expires after 15 minutes
and can be cancelled. Starting a new login cancels the person's older
pending one.

| Method | Path | Result |
|---|---|---|
| POST | `/api/model-connections/chatgpt/login` | Start. Body `{connectionId?}`; with an id, a completed login reconnects that connection |
| GET | `/api/model-connections/chatgpt/login/{id}` | Status: `pending`, `completed`, `failed`, `expired` or `cancelled` |
| DELETE | `/api/model-connections/chatgpt/login/{id}` | Cancel |
| PUT | `/api/model-connections/chatgpt/{id}` | `{name?, model, reasoningEffort}` (`low`, `medium`, `high`, `xhigh` or `max`) |

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
- When the backend answers 401, the Core reports the rejected token by its
  SHA-256 digest to
  `POST /internal/core/personas/{persona}/model/credential-refresh`. It then
  sends the identical request once more with the refreshed token. A second
  401 ends the turn.
- A refresh failure the issuer marks permanent (expired, reused or
  invalidated refresh token, `invalid_grant`, 401), or a token for another
  account, marks that one connection `reconnect_required`. The turn fails
  with `model_reconnect_required`, and the settings screen offers
  "ChatGPTに再接続". Other people's connections are unaffected.
- A token refresh keeps the connection version. A reconnect creates a new
  version.

## Model calls

The Core posts to `https://chatgpt.com/backend-api/codex/responses` with
`fetch` only. The same code runs under Node and workerd.

- Headers: `Authorization`, `ChatGPT-Account-ID`, `originator: sumi` and
  `session_id` (the persona).
- Body: `store: false`, `stream: true`, and no output-token bound.
- Models the Codex catalog marks "responses lite" (`gpt-6-*`, `gpt-5.6-*`,
  `gpt-daybreak-*`) use that request shape. They send the
  `x-openai-internal-codex-responses-lite` header, and the tools and
  instructions go in as developer items rather than top-level fields. The
  table is static, taken from `openai/codex` at commit 44fe510c.
- A 429 with `usage_limit_reached` or `usage_not_included` fails the turn
  with `model_usage_limit`, including the reset time when one is given.
  Other 429 responses are retried like any rate limit.

The protocol follows the public `openai/codex` client source. It has been
tested only against synthetic fixtures. The following are still unproven
against the live backend:

- whether it accepts `originator: sumi`;
- whether it requires client attestation;
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
