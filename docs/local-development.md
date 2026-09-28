# Local development

`make dev` starts PostgreSQL, the Go API, the TypeScript Core pool and Vite. It uses the same Core as Local host and Cloud. Node.js 22.18+, Go, pnpm, Docker, curl, openssl and flock are required. `make dev-check` validates configuration without starting processes.

Copy `deploy/local/.env.example` to the ignored `deploy/local/.env.local`, restrict it to mode 0600, and configure matching Firebase browser/Admin credentials. Use a real Firebase project or an explicitly configured emulator. The default Core provider is deterministic `mock`; a person's selected API connection overrides host defaults. `SUMI_MODEL_PROVIDER=openai` requires `SUMI_MODEL_BASE_URL`, `SUMI_MODEL_API_KEY` and `SUMI_MODEL_MODEL`.

Open exactly `http://127.0.0.1:5173`, or configure one exact reachable host and matching browser origins. Calls require the documented LiveKit configuration and a browser secure context. The launcher keeps command receipts, browser events and Messaging attachment bytes below `SUMI_REAL_STACK_STATE_ROOT` (default `~/.local/state/sumi/real-stack`); PostgreSQL persists in its Compose volume. Keep them together across restarts.

For the full container stack, `scripts/dev/compose-stack up` uses `deploy/local/compose.dev.yaml`. The API and Core pool share a network namespace so the pool can keep its loopback-only listener. The API wakes it at `http://127.0.0.1:8086`; an explicitly configured `SUMI_CORE_WAKE_URL` can target a Cloud host instead. Build/publish the matching API, Core, job, provisioner and Web images. Firebase and canonical files have their own deployment configuration. `compose-stack config` checks the assembled configuration; `down` stops the stack without deleting product state.

The initial schema requires an empty database. There is no Rust runtime option, generation RPC, old database importer or cutover mode. See [Core architecture](core-architecture.md).
