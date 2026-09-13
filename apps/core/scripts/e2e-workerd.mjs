#!/usr/bin/env node
/**
 * Workerd e2e: REAL local workerd (wrangler dev/miniflare) running the
 * SecretaryObject Durable Object → real Go state service → real PostgreSQL.
 *
 * Flow: state-dev up → persona provisioned → input submitted →
 * POST /personas/:id/wake on the worker → DO drains the turn →
 * outbox reply observed via the state API. Proves the workerd host adapter
 * runs the shared core end-to-end against canonical PG state.
 *
 * Requires SUMI_TEST_DB_URL. Offline: wrangler dev is local-only.
 *   node scripts/e2e-workerd.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error("e2e-workerd: SUMI_TEST_DB_URL required");
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const STATE_PORT = 8760 + (process.pid % 200);
const WORKER_PORT = 8760 + ((process.pid + 137) % 200);
const STATE = `http://127.0.0.1:${STATE_PORT}`;
const WORKER = `http://127.0.0.1:${WORKER_PORT}`;
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const personaId = uuidv7();

const log = (...a) => console.log("[e2e-w]", ...a);
const fail = (m) => {
  console.error("[e2e-w] FAIL:", m);
  cleanup();
  process.exit(1);
};
const assert = (c, m) => {
  if (!c) fail(m);
};

let svc, worker;
function cleanup() {
  try {
    svc?.kill("SIGKILL");
  } catch {}
  try {
    worker?.kill("SIGKILL");
  } catch {}
}
process.on("exit", cleanup);

async function sreq(method, path, token, body) {
  const res = await fetch(STATE + path, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return {
    status: res.status,
    json: await res.json().catch(() => null),
    text: await res.text().catch(() => ""),
  };
}
async function waitOk(url, ms) {
  while (Date.now() < ms) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 250));
  }
  fail(`${url} never became ready`);
}

// --- state-dev ---------------------------------------------------------
const binDir = mkdtempSync(join(tmpdir(), "sumi-e2ew-"));
const bin = join(binDir, "state-dev");
log("building state-dev…");
if (
  spawnSync("go", ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"], {
    cwd: API_DIR,
    stdio: "inherit",
  }).status !== 0
)
  fail("go build failed");
svc = spawn(bin, [], {
  env: {
    ...process.env,
    SUMI_DB_URL: DB_URL,
    SUMI_CORE_STATE_TOKEN: ADMIN,
    SUMI_STATE_LISTEN: `127.0.0.1:${STATE_PORT}`,
  },
  stdio: ["ignore", "pipe", "pipe"],
});
svc.stderr.on("data", (d) => process.stderr.write(`[state-dev] ${d}`));
await waitOk(`${STATE}/health`, Date.now() + 15_000);
log("state-dev up on", STATE);

const created = await sreq("POST", "/internal/core/personas", ADMIN, {
  persona_id: personaId,
  display_name: "workerd e2e",
});
assert(created.status === 201, `createPersona ${created.status}`);
const ptoken = created.json.persona_token;

// --- wrangler dev --------------------------------------------------------
// Per-persona capability delivered as a dev var (production: secret).
const devVars = join(binDir, ".dev.vars");
writeFileSync(
  devVars,
  `SUMI_PERSONA_TOKEN_${personaId.replaceAll("-", "_")}=${ptoken}\n`,
);
log("starting wrangler dev (local workerd) on", WORKER);
worker = spawn(
  "pnpm",
  [
    "exec",
    "wrangler",
    "dev",
    "--config",
    "wrangler.jsonc",
    "--port",
    String(WORKER_PORT),
    "--ip",
    "127.0.0.1",
    "--var",
    `SUMI_STATE_URL:${STATE}`,
    "--var",
    `SUMI_PERSONA_TOKEN_${personaId.replaceAll("-", "_")}:${ptoken}`,
    "--show-interactive-dev-session=false",
  ],
  {
    cwd: CORE_DIR,
    env: { ...process.env, CI: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  },
);
worker.stdout.on("data", (d) => process.stdout.write(`[workerd] ${d}`));
worker.stderr.on("data", (d) => process.stderr.write(`[workerd] ${d}`));
await waitOk(`${WORKER}/health`, Date.now() + 45_000);
log("workerd up");

// --- scenario: input → worker wake → DO drains → outbox ------------------
const sub = await sreq(
  "POST",
  `/internal/core/personas/${personaId}/inputs`,
  ptoken,
  {
    input_id: `in-${randomUUID()}`,
    kind: "message",
    payload: { text: '!journal.note {"text":"woke in workerd"}' },
    actor_kind: "human",
    actor_id: "e2e-w",
    source_surface: "workerd-e2e",
  },
);
assert(sub.status === 201, `submitInput ${sub.status}`);

const wake = await fetch(`${WORKER}/personas/${personaId}/wake`, {
  method: "POST",
});
assert(wake.ok, `wake ${wake.status}: ${await wake.text()}`);
log("wake accepted; waiting for outbox…");

let reply = null;
for (let i = 0; i < 40; i++) {
  await new Promise((r) => setTimeout(r, 500));
  const ob = await sreq(
    "GET",
    `/internal/core/personas/${personaId}/outbox?after_seq=0`,
    ptoken,
  );
  const found = (ob.json?.outbox ?? []).find(
    (o) => o.payload?.input_id === sub.json.input.input_id,
  );
  if (found) {
    reply = found;
    break;
  }
}
assert(reply, "no turn_completed outbox entry via workerd path");
const toolResults = reply.payload.output?.tool_results ?? [];
assert(
  toolResults.some((r) => r.tool === "journal.note"),
  `no journal.note result in ${JSON.stringify(reply.payload)}`,
);
const evs = await sreq(
  "GET",
  `/internal/core/personas/${personaId}/events?after_seq=0`,
  ptoken,
);
const note = (evs.json?.events ?? []).find((e) => e.kind === "note");
assert(note, "journal.note effect missing");
assert(
  note.payload.text === "woke in workerd",
  `note text ${JSON.stringify(note.payload)}`,
);
log("reply:", reply.payload.output.text);
log("journal note:", note.payload.text);
log("PASS — workerd DO ran the shared core against real Go+PG");
cleanup();
process.exit(0);
