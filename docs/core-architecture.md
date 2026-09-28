# Core architecture

People and their AI secretaries share applications and a workspace. A secretary's identity and accumulated state continue across process restarts and current Local↔Cloud moves.

`apps/core` is the only secretary runtime. It runs as a Node.js host locally, or as a Cloudflare Durable Object. The Go API owns PostgreSQL state: persona identity, inputs, turns, memory, approvals, effects, schedules, jobs and placement. Committed inputs and operation receipts drive recovery after interruption.

The Web app uses authenticated Go APIs for shared Messaging, workspace membership, files, browser tabs and terminals. Direct Chat admits each authenticated command into a durable receipt log and submits a corresponding Core input. Core journal events are projected into the browser event journal for history, WebSocket replay and reconnect cursors. These are current presentation/receipt stores, not a second secretary runtime. At a full deployment reset, reset PostgreSQL and both journals together; retained commands must never be paired with an unrelated empty Core.

Cloud jobs and interactive terminals use the process provisioner. It owns Docker access and runs the pinned job image against a verified canonical files scope. It cannot prepare, activate or recover an old secretary runtime. The API itself has no Docker socket. Browser tabs, per-user model connections and MCP connections retain their own current authorization boundaries.

Fresh installations apply `apps/api/internal/db/migrations/0001_core.up.sql`. The pre-Core databases, Rust executable, SQLite wrapping keys, runtime-generation RPCs and old-to-new cutover are retired. No importer or backward compatibility mode exists. Ordinary future PostgreSQL migrations can be added after the initial schema.

Current secretary portability is separate: [Local→Cloud](local-cloud-move.md), [Cloud→Local](cloud-local-return.md), [portable state](agent/portable-state.md). It preserves the same current Core secretary and uses explicit placement/transfer ownership.

See [Local host](local-host.md), [source development](local-development.md), [shared browser host](shared-browser-host.md), [Core jobs](agent/core-jobs.md) and [external MCP](agent/external-mcp.md) for supported capabilities and setup.
