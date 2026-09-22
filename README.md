# Sumi
A shared workspace where people and their AI secretaries work together.

English | [日本語](README.ja.md)

### Description

Sumi brings conversations, tasks, calendars, notes, email, browsing, meetings, studying, and whatever else each person needs into one connected place.

The workspace takes shape around each person's routines and needs. As trust grows, their AI secretary can remain by their side and understand more of their everyday life and work as it unfolds.

The interface can adapt to what each moment calls for. AI secretaries can coordinate with people and one another, point things out on screen, and gradually take action with the permissions people give them. People and their AI secretaries use the same apps and inhabit the same workspace.

Each AI secretary lives there as an individual, moving through time alongside the people around them. What they live through together becomes part of who each secretary is and who they are becoming.

Sumi aims to democratize access to personal secretaries and extend what a personal secretary can be.

## Where Sumi is today

Sumi is in alpha. The Description above is the goal. Today there are three ways to try Sumi, with different interfaces and setup requirements.

| Way to see Sumi | What it is today | Who can use it |
|---|---|---|
| **Hosted alpha Web app** | The existing Web app, with invite-only sign-in, Workspaces and Messaging, deployed together with the new secretary core at the current alpha build. | Invited developers and testers only; there is no public sign-up. Everyday sign-in-to-secretary use of the hosted app on the new core remains open acceptance work. |
| **[Local host](#try-the-local-host)** | The new secretary core on one Linux or WSL machine, without a Sumi Cloud account. | Anyone who installs it — from a source checkout or a `sumi-local pack` bundle (linux/amd64; no Go needed for a bundle). Its browser page and `say` command are engineering surfaces, not the product UI. |
| **[Web app from source](#run-the-web-app-from-source)** | The full Web app — sign-in, Workspaces, Messaging and settings — with secretaries on the TypeScript secretary core. | Developers with their own Firebase project; no model credential is needed for the default deterministic provider. |

### The new secretary core

The new core is what the Local host runs and what Sumi Cloud is moving to. Its source and tests establish the following.

- **A secretary's state outlives the process running it.** Identity, conversation history, queued and in-progress requests, and schedules are saved in PostgreSQL through the Go API. After a stop or crash, the next start continues interrupted requests from their saved progress instead of starting them over. On the Local host, the same secretary also returns after `uninstall` and reinstall, as long as the state home is kept. See [Local host semantics](docs/local-host.md#semantics).
- **One core for Local and Cloud.** The core runs as a Node.js process on your machine or, for Sumi Cloud, as one Cloudflare Durable Object per secretary. Both read and write state through the Go API.
- **A secretary chooses how to take part in a conversation.** Messaging passes the secretary the messages its notification settings let through. A direct message, a mention, a reply to the secretary's own message, or a reminder the secretary set asks for a response; other messages arrive for awareness. The secretary decides whether to speak, so not every message gets a reply.
- **Operations that need permission wait for a person.** The request waits in that person's approval inbox. Tests of the approval flow check that an approved operation resumes the waiting request and runs once, that a denied operation does not run, and that another person cannot see or decide it.
- **Each person chooses their secretary's model.** A person connects their own API key through a supported preset, such as an OpenAI-compatible chat completions endpoint, OpenAI Responses, Anthropic Messages or OpenCode Go. The core uses exactly that connection; if it cannot be used, the request fails rather than being answered by a different model. Model usage is recorded.

### Current limits

- **Using the hosted Web app on the new core as a product.** `make dev` runs the Web app from source on the new core for development — Direct Chat, shared Messaging history/search/post/edit, and workspace invitations all run through the durable core. The hosted alpha deployment already serves the accepted API, Web and secretary core together, but product-level use of it (a real person's sign-in driving a secretary day to day) is not yet accepted.
- **Verified end-to-end use of the Local→Cloud move.** The product surface exists — the sign-in screen carries a secretary-move panel — and the state transfer itself has been exercised once against the live Cloud alpha: the same secretary activated on Cloud with its pending input carried over, the Local install then answered `secretary_moved`, and a staged cancellation restored the Local secretary. That run covered state only — files do not move. Its invitation was seeded through a guarded CLI and the external sign-in proof was substituted for the test, while the transfer, confirmation and post-move requests themselves ran against the real public endpoints. The UI flow has not been exercised end to end.
- **The rest of the apps in the Description.** Messaging is currently the only Workspace app. Tasks, calendars, notes, email, browsing, meetings and studying are not yet apps that people and secretaries share.
- **Secretaries speaking in calls.** Call support in the source is opt-in and uses LiveKit. A secretary's call participation does not yet have a real speech-recognition engine.
- **Shared browser and desktop apps.** `apps/desktop` contains an engineering Electron browser runtime. With an existing Sumi session configured, a person can grant their secretary access to the same visible tab through the Core's browser tools; see [configured browser host](docs/shared-browser-host.md). The connected path has been exercised with real Electron, API and PostgreSQL on Linux/WSL, using fixture login proof and a deterministic model. Desktop sign-in, product browser chrome, Mac/Windows packaging and native mobile apps remain unfinished. `apps/web` remains the intended shared product renderer ([ADR 0014](docs/adr/0014-webapp-and-electron-runtime.md)).
- **External MCP tools.** The main API can store a person's granted remote HTTPS MCP connections and execute their tools through durable Core jobs; see [MCP configuration and limits](docs/agent/external-mcp.md). This source implementation is under integration review and is not yet enabled in the hosted alpha. The account-free Local host also supports saved HTTPS connections and explicitly configured stdio servers through `sumi-local mcp`; see [Local configuration](docs/local-host.md). Real binary/CLI/Core/protocol-server tests use a deterministic model on Linux. OAuth and MCP Apps rendering remain open.
- **Signing in with a ChatGPT/Codex subscription in the new core.** Use an API-key connection instead.

## Try the Local host

`deploy/local-host/sumi-local` installs and runs the new secretary core on one machine. It runs two processes: a Go state service backed by PostgreSQL, and the secretary. With no model configured, the secretary uses a `mock` model that echoes your message, so you can try restarts and recovery before connecting a real model.

Requirements: Linux (use WSL on Windows), bash 5 or newer, Node.js 22.18 or newer (or 23.6 or newer), `curl`, `openssl`, `flock` and `tar`, and either Docker or a PostgreSQL database you provide. Source installs and bundle builds need Go and the installed workspace frontend dependencies (`pnpm install --frozen-lockfile`). A `sumi-local pack` bundle includes the shared terminal assets and needs no Go, pnpm, or repository to install.

```sh
deploy/local-host/sumi-local install --managed-pg   # or: --db-url postgres://…
sumi-local doctor
sumi-local start
sumi-local say --wait 60 "hello"
sumi-local status
sumi-local stop
```

`install` adds a `sumi-local` command to `~/.local/bin` once the install completes — a failed install leaves an existing shim pointing at the previous install. `sumi-local url` prints the browser address; it contains the page's access token, so treat it like a password. `sumi-local uninstall` keeps your secretary's data, and `sumi-local uninstall --purge` deletes it. `sumi-local pack` builds a bundle you can install on another Linux (amd64) machine without Go; no prebuilt bundle is published.

To connect a real model, set `SUMI_MODEL_PROVIDER=openai` and the `SUMI_MODEL_*` values for an OpenAI-compatible endpoint in the install's `config.env`. The Local host runs one secretary per install. See [Local host](docs/local-host.md) for configuration, recovery behavior and current limits.

## Run the Web app from source

`make dev` starts the full Web app with real secretaries on your machine: the Go API, PostgreSQL in Docker, the accepted TypeScript secretary core (`apps/core`) via a local dev pool — the same core the Local host and Sumi Cloud use — and Vite. `make dev-rust` keeps the Rust agent runtime and tool executor (`apps/agent`) as an explicit diagnostic path.

Requirements: Node.js 22.18 or newer, pnpm 11, Go, Docker, `curl`, `openssl` and `flock`; a Firebase project with Google or GitHub sign-in and matching Admin credentials. The default core runtime needs no model credential (deterministic `mock` provider); `make dev-rust` additionally requires Rust stable and model-provider credentials for the conversation model and two separate review models.

```sh
make setup
cp deploy/local/.env.example deploy/local/.env.local
chmod 600 deploy/local/.env.local
# Fill in the Firebase, identity and model values described in docs/local-development.md
make dev-check
make dev
```

Open exactly <http://127.0.0.1:5173>. [Real local stack](docs/local-development.md) explains each setting, access from another Tailnet device, calls and recovery.

## Repository layout

```text
apps/
  web/                React Web app: the single renderer for Web, mobile WebApp and a future desktop app
                      (cloudflare/ holds the edge Worker for serving the Web app on Cloudflare)
  api/                Go API: sign-in sessions, identity, Workspaces, Messaging, approvals,
                      model connections, usage, and the state service the secretary core uses
  core/               TypeScript secretary core: Node.js hosts and the Cloudflare Durable Object host
  desktop/            Engineering Electron shared browser runtime and authenticated Core host connection
  agent/              Rust agent runtime and isolated tool executor used by `make dev-rust`
packages/
  ui/                 @sumi/ui component catalog (based on shadcn/ui)
  sdui/               @sumi/sdui declarative UI schema (zod) and renderer
  api-client/         @sumi/api-client types generated from contracts/
  typescript-config/  shared tsconfig presets
contracts/            OpenAPI and agent event schemas
deploy/               local-host/ (sumi-local), local/ (development Compose), service Dockerfiles, firebase/
docs/                 ADRs, design notes and runbooks
scripts/              development, acceptance and operations scripts
CONTEXT.md            domain glossary (Japanese)
```

## Technology

| Area | Technology |
|---|---|
| Web app | React 19, TypeScript, Vite, TanStack Router, Tailwind CSS v4, Zustand, zod |
| UI components | `@sumi/ui` (based on shadcn/ui), `@sumi/sdui` |
| Sign-in | Firebase Authentication, with sessions issued by the Go API |
| API and canonical state | Go, PostgreSQL |
| Secretary core | TypeScript on Node.js (Local) and Cloudflare Workers Durable Objects (Cloud) |
| Transitional agent runtime (`make dev-rust`) | Rust |
| Calls | LiveKit |
| Contracts | OpenAPI, JSON Schema |
| Monorepo and tooling | pnpm workspaces, Turborepo, Biome |
| Build configuration | GitHub Actions workflows; local container-image builds (`scripts/operations/build-dogfood-images`, `deploy/*/Dockerfile`) |

## Development commands

```sh
make build     # build all apps and packages
make lint      # Biome, go vet, cargo clippy and rustfmt
make test      # all tests
make format    # format the repository
make db-up     # start the development PostgreSQL in Docker
make migrate   # apply API schema migrations (requires SUMI_DB_URL)
```

After editing `contracts/openapi.yaml` or `contracts/agent-events.yaml`, regenerate the TypeScript types with `pnpm --filter @sumi/api-client generate`. `make dev-workspaces` only runs each package's raw dev task; it does not start a usable Sumi.

The workflows in `.github/workflows/` are configured to run on every pull request and on pushes to `main`: Web and shared TypeScript checks, Web edge contracts, API contracts against PostgreSQL, the secretary core, and the Rust agent.

## Design principles

- **People and secretaries use the same apps.** A secretary works through the same applications, operations and authorization checks as people, not through a separate agent-only copy of the product ([ADR 0008](docs/adr/0008-personality-agent-identity-and-execution-fabric.md), [ADR 0011 (proposed)](docs/adr/0011-messaging-surface-and-agent-participation.md), [ADR 0013](docs/adr/0013-tool-invocation-routes-and-authority-provenance.md)).
- **A secretary is one continuing individual.** People and secretaries are registered in the same identity registry. Starting or stopping the process that runs a secretary is resource management; it is not the secretary sleeping or ending ([ADR 0009](docs/adr/0009-human-koseki-and-multi-user-auth.md), [CONTEXT.md](CONTEXT.md)).
- **Canonical state lives behind the API.** The processes that run a secretary keep no canonical state, so they can be stopped, restarted or replaced, and the next process recovers from what was saved.
- **A person's model choice is authoritative.** The new secretary core does not substitute an operator model for the connection a person selected.
- **One renderer and one contract source.** `apps/web` is the only application renderer, and `contracts/` defines the API and event schemas shared across languages.

## Documentation

- [CONTEXT.md](CONTEXT.md) — domain terms such as Human, Secretary, Workspace and Hire (Japanese)
- [docs/adr/](docs/adr/) — architecture decisions
- [Local host](docs/local-host.md) — installing and running the new secretary core without a Cloud account
- [Real local stack](docs/local-development.md) — running the full Web app from source
- [Portable secretary state](docs/agent/portable-state.md) — the foundation for moving a secretary between Local and Cloud
- [Secretary core on Cloudflare](docs/operations/cloud-core-alpha.md) — how the Cloud secretary core is deployed and verified (operators)
- [Alpha acceptance status](docs/operations/alpha-acceptance.md) — what is verified, exercised with mocks, and still untested
- [Roadmap](docs/roadmap.md)
