# Real local stack

This is the supported developer entrypoint for using the browser chat with one
real `PersonalityAgent`. It runs the Go API, Rust tool executor, Rust production
runtime, and Vite as native processes. The Playwright stack remains a test
fixture and is not the product entrypoint.

## Prerequisites

- Node.js 20.19 or newer, pnpm 11, Go, Rust stable, `curl`, `openssl`, and
  `flock`
- `pnpm install` (`make setup`) completed
- a Firebase project with Authentication enabled
- Google and/or GitHub enabled under Firebase Authentication → Sign-in method
- a real model-provider credential for a preset supported by `apps/agent`

The Vite development build has a public `sumi-studio` Firebase web
configuration fallback. It is an identifier, not a server credential, and is
never selected implicitly by a production build. Analytics is not initialized.
Production builds, or local development against a different Firebase project,
must set all four `VITE_FIREBASE_API_KEY`, `VITE_FIREBASE_AUTH_DOMAIN`,
`VITE_FIREBASE_PROJECT_ID`, and `VITE_FIREBASE_APP_ID` values. The launcher
rejects a web project that differs from the Admin project.

For Google login, enable the Google provider. For GitHub login, configure its
OAuth client ID and secret in Firebase and register the callback URL shown by
Firebase. Add the exact browser host (`127.0.0.1`, or the selected literal
Tailnet IPv4 address) to Firebase Authentication's authorized domains.

## Firebase Admin ADC and the identity binding

The Go API verifies Firebase ID tokens with Firebase Admin Application Default
Credentials (ADC). Use one of:

```sh
export GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account.json
```

or:

```sh
gcloud auth application-default login
gcloud config set project sumi-studio
```

The service-account JSON or active `gcloud` project must match
`SUMI_AUTH_FIREBASE_PROJECT_ID`. The launcher validates this without printing
the credential. It cannot prove IAM permission statically: the credential must
also be authorized to verify tokens and check revocation in that project. The
Go server performs the real Admin SDK initialization and fails startup when
that access is unavailable.

An already-running Firebase Auth emulator remains an explicit alternative for
local testing: set both `FIREBASE_AUTH_EMULATOR_HOST=<literal-ip>:<port>` for
Firebase Admin and
`VITE_FIREBASE_AUTH_EMULATOR_URL=http://<literal-ip>:<port>` for the browser.
For direct Tailnet access, the browser endpoint must use the same Tailnet IP as
`SUMI_PUBLIC_LISTEN`; `127.0.0.1` would refer to the remote browser's machine,
not this host. The launcher validates and passes both endpoints, but does not
start or expose the emulator. ADC validation is skipped only in this explicit
emulator mode.

Copy the template, then fill the required blanks:

```sh
cp deploy/local/.env.example deploy/local/.env.local
chmod 600 deploy/local/.env.local
```

`SUMI_AUTH_FIREBASE_UID` is the exact UID shown for the authorized user in
Firebase Authentication. A tenant-aware Identity Platform user also requires
the exact `SUMI_AUTH_FIREBASE_TENANT_ID`; leave it blank for ordinary Firebase
Auth. `SUMI_AUTH_TENANT_ID` and `SUMI_AUTH_USER_ID` are server-owned Sumi
identifiers, not claims accepted from the browser.

The following identity must be equal everywhere:

```text
SUMI_PERSONALITY_AGENT_ID
  = SUMI_AUTH_PERSONALITY_AGENT_ID
  = local-control PersonalityAgent identity
  = executor/runtime PersonalityAgent identity
```

The launcher derives the local-control and executor/runtime values from
`SUMI_PERSONALITY_AGENT_ID` and rejects an unequal auth binding. The ID must be
a canonical lowercase UUIDv7.

For the model, `SUMI_MODEL_API_KEY_ENV` names the credential variable. For
example, the template selects `opencode-go` and therefore requires:

```text
SUMI_MODEL_PRESET=opencode-go
SUMI_MODEL_API_KEY_ENV=OPENCODE_GO_API_KEY
OPENCODE_GO_API_KEY=<real credential>
```

The launcher never prints Firebase or provider credentials.

Production AutoReview has no conversation-model fallback. Before spawning a
PersonalityAgent, configure both `SUMI_EXECUTION_REVIEWER_MODEL_*` and
`SUMI_ESCALATION_REVIEWER_MODEL_*`, and provide
`SUMI_EXECUTION_REVIEWER_API_KEY` and `SUMI_ESCALATION_REVIEWER_API_KEY` from
separate secret sources. The resolved reviewer origins and credential variable
names must be distinct from the conversation model and from each other. This
repository deliberately does not select those providers for dogfood. A
declared `account_scope` remains part of the trust identity, but its text alone
does not prove that two credentials belong to separate provider accounts.

### Codex OAuth bridge provider

The existing development-only Codex Responses bridge can provide a real model
without a public OpenAI API key. It reads an owner-only Codex login file,
stays bound to loopback, authenticates Sumi with a separate ephemeral secret,
and substitutes OAuth only on the upstream request.

In one host terminal:

```sh
export SUMI_CODEX_RESPONSES_PROXY_SECRET="$(openssl rand -hex 32)"
python3 scripts/dev/codex-responses-proxy.py --auth-file ~/.codex/auth.json
```

Set these model values in `deploy/local/.env.local`:

```text
SUMI_MODEL_PRESET=openai-responses
SUMI_MODEL_ID=gpt-5.6-sol
SUMI_MODEL_BASE_URL=http://127.0.0.1:8765/v1
SUMI_MODEL_API_KEY_ENV=SUMI_CODEX_RESPONSES_PROXY_SECRET
```

Then run `make dev` from a shell that retains
`SUMI_CODEX_RESPONSES_PROXY_SECRET`. The production runtime permits this
literal-loopback HTTP provider override; it still rejects non-loopback HTTP.
This bridge is a development path to the ChatGPT Codex subscription endpoint,
not a public OpenAI API-key or production-provider contract.

## Start and verify

### Pre-cutover Workspace database reset boundary

Migration `0008_workspace_core` intentionally replaces the earlier
pre-dogfooding `0008_messaging_schema`. A database that has recorded the old
version is not upgraded, backfilled, or adopted: the migrator stops with an
explicit reset-required error before applying `0009`.

Before Developer Workspace contains data that must survive, reset that local
database and migrate from empty:

```sh
docker compose -f deploy/local/compose.yaml down -v
make db-up
make migrate
```

The `down -v` command deletes the local control-plane volume. Do not run it
against a database whose contents must be retained; this boundary exists only
for the current pre-cutover environment.

This exception ends immediately before the first real Developer Workspace
message. Rehearse an empty deploy, insert a representative Workspace / member /
installation / message / notification-intent chain, restore a database backup
into a second empty database, and verify both the rows and the latest migration
version. Then seal the exact migration bytes:

```sh
node scripts/dev/migration-freeze.mjs seal
(cd apps/api && go test ./internal/db)
```

Commit `apps/api/internal/db/migrations/FROZEN.sha256` before allowing that
first message. From that commit onward, every migration file named in the
manifest is immutable and all schema changes use a new forward migration. The
ordinary database test suite validates the seal; `node
scripts/dev/migration-freeze.mjs check` provides the same explicit operator
check. A later forward migration must be added to the manifest with
`node scripts/dev/migration-freeze.mjs extend`; that command refuses to extend
over any changed or deleted sealed entry, a numeric gap at or below the sealed
maximum, or anything other than one matching `up`/`down` pair at a new higher
version. Resetting or replacing an applied migration is no longer an available
operation after this point.

First validate configuration, then start:

```sh
make dev-check
make dev
```

Open exactly <http://127.0.0.1:5173>. The fixed Vite server proxies HTTP
`/auth` and WebSocket `/direct-chat` to <http://127.0.0.1:8080>, so the browser
uses one origin for session cookies and chat. The API allowlist is set to the
exact origin `http://127.0.0.1:5173`. `SUMI_AUTH_ALLOW_INSECURE_COOKIES=true`
is set only by this native HTTP launcher; production keeps secure cookies and
its exact-origin rules.

### Direct Tailnet access

To make the same native processes reachable from another Tailnet device,
change only the literal bind in `deploy/local/.env.local`:

```text
SUMI_PUBLIC_LISTEN=<this-host-tailscale-ipv4>:8080
```

The launcher verifies that the address is a locally bindable Tailnet IPv4
inside `100.64.0.0/10`, rejects LAN addresses, hostnames, `0.0.0.0`, and other
wildcard exposure, then derives:

```text
API listen and proxy target  http://<tailscale-ipv4>:8080
Vite listen/browser origin   http://<tailscale-ipv4>:5173
API browser origin allowlist http://<tailscale-ipv4>:5173
```

It also inspects the native listeners and aborts if either server widened to an
all-interface socket. No Tailscale Serve, firewall change, or OS networking
mutation is performed. Open `http://<tailscale-ipv4>:5173` from the other
device.

Tailnet HTTP intentionally uses the local-only insecure-cookie flag because
the browser sees an `http://` origin. Authentication and direct chat still use
one exact origin and an HttpOnly session cookie; this does not change the
production HTTPS/secure-cookie contract.

Before the first direct Tailnet login, the selected literal IP must be added to
the Firebase project's authorized domains and the Admin ADC above must have
access to the same project. A localhost-only Firebase domain configuration or
an ADC identity without project permission will not pass the human smoke.

The production Rust connector deliberately accepts plaintext WebSockets only
to a literal loopback address. When the API is bound to the Tailnet address,
the launcher therefore adds a loopback-only TCP relay at `127.0.0.1:8082` for
the runtime-to-API gateway connection. The relay does not listen on the
Tailnet, does not change the browser path, and is stopped with the stack.

Startup order and gates are:

```text
API /health → loopback gateway relay → executor Unix socket
  → authenticated runtime Ready → Vite
```

The Ready gate observes the integrity-protected local-control state produced
after the runtime authenticates to the control plane and completes the
executor Health exchange. Vite is not started if any earlier gate fails.

The launcher also generates one ephemeral Ed25519 call-authority pair for the
disposable generation. Only the private seed is passed to the runtime; only
the corresponding public key is passed to the executor. Neither key is read
from the developer env file, and the broker receives neither half. The
production supervisor follows the same role split with allocator-owned,
per-generation identities.

The real-browser agent fixture goes beyond the Health gate. Its first provider
request returns a Normal `list_dir` call, the built-in Normal policy
deterministically allows that bounded read without invoking the external
Execution AutoReviewer, and the post-COMMIT runtime permit is signed and
verified by the executor. The next provider request must contain the exact
directory result for a fixture-owned probe file before it returns the first
assistant text; a third request then verifies that the second Human turn
retained the complete tool/result and conversation context.

Human credential-gated smoke:

1. Sign in with the enabled Google or GitHub provider.
2. Confirm the chat reports `エージェント利用可能`.
3. Send a message and confirm a real provider response streams into the chat.
4. Press Ctrl-C and confirm the API, relay, executor, runtime, and Vite stop.

The automated checks validate configuration, proxying, identity equality,
workspace propagation, startup gates, and the local exact-call executor path.
They do not perform the final third-party Firebase/provider login and
billing-bearing model request.

### Calls need a secure origin

Browsers expose the microphone only on a secure context, so DM calls and voice
channels work from `http://127.0.0.1:5173` (loopback counts as secure) and from
an `https://` origin, but not from a plain `http://<tailscale-ipv4>:5173` page.
The UI states this explicitly and refuses to join before requesting a call
token; text messaging is unaffected. To use calls from another device, front
the stack with HTTPS instead of the direct Tailnet HTTP endpoint:

- terminate TLS on a name you control (for example Tailscale Serve on the host)
  and proxy it to the Vite origin; list the origin in `SUMI_DEV_PUBLIC_ORIGINS`
  (`scripts/dev/compose-stack`), which admits it as a browser origin and Vite
  host;
- proxy LiveKit's signalling port (7880) the same way and set `SUMI_LIVEKIT_URL`
  to that `wss://` address — an `https://` page cannot use `ws://` signalling and
  the UI reports it as mixed content;
- add the name to the Firebase project's authorized domains, as for the literal
  Tailnet IP above.

Media (UDP 7882, TCP fallback 7881) is not proxied; it goes straight to the
bind host, which the other device reaches over the Tailnet.

## Disposable generation boundary

`make dev` creates state directories and non-production secrets in one
mode-0700 temporary directory. It acquires one host lock, starts exactly one
generation (`0`), never restarts or replaces that generation, and deletes all
state on shutdown. Because there is no surviving ledger or replacement
generation to allocate, the persistent supervisor allocator is not part of
this deliberately disposable single-agent direct path.

Do not use this exception for persistent, concurrent, or restartable
deployment. `deploy/agent/supervisor` and its allocator own monotonic
generation allocation for that case.

`make dev-workspaces` is retained only for raw package development. It does not
orchestrate an authenticated usable Sumi stack.
