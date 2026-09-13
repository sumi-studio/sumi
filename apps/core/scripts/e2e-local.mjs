#!/usr/bin/env node
/**
 * E2E: REAL Go state service + REAL PostgreSQL + real Node core process.
 * Covers: persona provision → submit input → drain → outbox; crash mid-turn
 * (SIGKILL) → restart → exactly-once recovery; schedule → wake input.
 *
 * Requires SUMI_TEST_DB_URL (or SUMI_DB_URL). Builds the state-dev binary.
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:55432/sumi?sslmode=disable \
 *     node scripts/e2e-local.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error(
    "e2e: SUMI_TEST_DB_URL (or SUMI_DB_URL) required — real PostgreSQL",
  );
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const PORT = 8390 + (process.pid % 1000);
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;

// The uuidv7 domain requires version-7 UUIDs. Mint one.
function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const personaId = uuidv7();

function log(...a) {
  console.log("[e2e]", ...a);
}
function fail(msg) {
  console.error("[e2e] FAIL:", msg);
  process.exit(1);
}
async function dump(msg) {
  console.error("[e2e] FAIL:", msg);
  try {
    const [evs, st, ob] = await Promise.all([
      req(
        "GET",
        `/internal/core/personas/${personaId}/events?after_seq=0`,
        ptoken,
      ),
      req("GET", `/internal/core/personas/${personaId}/state`, ptoken),
      req(
        "GET",
        `/internal/core/personas/${personaId}/outbox?after_seq=0`,
        ptoken,
      ),
    ]);
    console.error("[e2e] events:", JSON.stringify(evs.json, null, 1));
    console.error("[e2e] state:", JSON.stringify(st.json));
    console.error("[e2e] outbox:", JSON.stringify(ob.json));
  } catch (e) {
    console.error("[e2e] dump failed:", e);
  }
  process.exit(1);
}
function assert(cond, msg) {
  if (!cond) fail(msg);
}
const assertD = async (cond, msg) => {
  if (!cond) await dump(msg);
};

async function req(method, path, token, body) {
  const res = await fetch(BASE + path, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* non-JSON */
  }
  return { status: res.status, json, text };
}

async function waitHealth(deadlineMs) {
  while (Date.now() < deadlineMs) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return;
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  fail("state-dev did not become healthy");
}

const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-"));
const bin = join(binDir, "state-dev");
log("building state-dev…");
const build = spawnSync(
  "go",
  ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"],
  {
    cwd: API_DIR,
    stdio: "inherit",
  },
);
if (build.status !== 0) fail("go build failed");

log("starting state-dev on", BASE);
const svc = spawn(bin, [], {
  env: {
    ...process.env,
    SUMI_DB_URL: DB_URL,
    SUMI_CORE_STATE_TOKEN: ADMIN,
    SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
  },
  stdio: ["ignore", "pipe", "pipe"],
});
svc.stderr.on("data", (d) => process.stderr.write(`[state-dev] ${d}`));
process.on("exit", () => svc.kill("SIGKILL"));

await waitHealth(Date.now() + 15_000);

// --- scenario 1: provision + input → drain → outbox ----------------------
log("scenario 1: provision persona, submit input, drain");
const created = await req("POST", "/internal/core/personas", ADMIN, {
  persona_id: personaId,
  display_name: "e2e secretary",
});
assert(
  created.status === 201,
  `createPersona ${created.status}: ${created.text}`,
);
const ptoken = created.json.persona_token;
assert(ptoken?.startsWith("core_"), "persona token missing");

// persona-scoped token must NOT work for a different persona
const other = uuidv7();
await req("POST", "/internal/core/personas", ADMIN, { persona_id: other });
const scopedOut = await req(
  "GET",
  `/internal/core/personas/${other}/state`,
  ptoken,
);
assert(
  scopedOut.status === 401,
  `cross-persona token accepted (${scopedOut.status})`,
);

const sub = await req(
  "POST",
  `/internal/core/personas/${personaId}/inputs`,
  ptoken,
  {
    input_id: `in-${randomUUID()}`,
    kind: "message",
    payload: { text: "hello from e2e" },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  },
);
assert(sub.status === 201, `submitInput ${sub.status}: ${sub.text}`);
// replay same input_id → idempotent 200, no second row
const replay = await req(
  "POST",
  `/internal/core/personas/${personaId}/inputs`,
  ptoken,
  {
    input_id: sub.json.input.input_id,
    kind: "message",
    payload: { text: "hello from e2e" },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  },
);
assert(replay.status === 200, `input replay not idempotent (${replay.status})`);

const env = {
  ...process.env,
  SUMI_STATE_URL: BASE,
  SUMI_PERSONA_ID: personaId,
  SUMI_PERSONA_TOKEN: ptoken,
  SUMI_MODEL_PROVIDER: "mock",
  SUMI_LEASE_TTL_MS: "4000", // short TTL so the kill test recovers quickly
};
const once = spawnSync(
  "node",
  [join(CORE_DIR, "src/host/local.ts"), "--once"],
  { env, encoding: "utf8", timeout: 60_000 },
);
if (once.status !== 0) {
  console.error(once.stdout, once.stderr);
  fail("local --once exited nonzero");
}
const out = await req(
  "GET",
  `/internal/core/personas/${personaId}/outbox?after_seq=0`,
  ptoken,
);
assert(
  out.json.outbox.length === 1,
  `expected 1 outbox reply, got ${out.json.outbox.length}`,
);
assert(
  out.json.outbox[0].kind === "turn_completed",
  `unexpected outbox kind ${out.json.outbox[0].kind}`,
);
assert(
  /echo: hello from e2e/.test(out.json.outbox[0].payload.output.text),
  "reply text mismatch",
);
log("  outbox reply:", out.json.outbox[0].payload.output.text);

// --- scenario 2: schedule → wake input ------------------------------------
log("scenario 2: schedule.set → due dispatch → wake turn");
const sub2 = await req(
  "POST",
  `/internal/core/personas/${personaId}/inputs`,
  ptoken,
  {
    input_id: `in-${randomUUID()}`,
    kind: "message",
    payload: {
      text: `!schedule.set {"wake_at":"+50","payload":{"text":"follow-up ping"}}`,
    },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  },
);
assert(sub2.status === 201, `submitInput sched ${sub2.status}`);
await new Promise((r) => setTimeout(r, 150));
const once2 = spawnSync(
  "node",
  [join(CORE_DIR, "src/host/local.ts"), "--once"],
  { env, encoding: "utf8", timeout: 60_000 },
);
if (once2.status !== 0) {
  console.error(once2.stdout, once2.stderr);
  fail("once2 failed");
}
const evs = await req(
  "GET",
  `/internal/core/personas/${personaId}/events?after_seq=0`,
  ptoken,
);
const wake = evs.json.events.find(
  (e) => e.kind === "input_received" && e.payload.actor_kind === "schedule",
);
await assertD(wake, "no scheduled wake input event");
log("  wake input delivered; total events:", evs.json.events.length);

// --- scenario 3: SIGKILL mid-turn → restart → exactly-once -----------------
log("scenario 3: kill mid-turn, restart, verify exactly-once");
const sub3 = await req(
  "POST",
  `/internal/core/personas/${personaId}/inputs`,
  ptoken,
  {
    input_id: `in-${randomUUID()}`,
    kind: "message",
    payload: { text: "!slow 8000 survivors" },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  },
);
assert(sub3.status === 201, `submitInput slow ${sub3.status}`);
const runner = spawn("node", [join(CORE_DIR, "src/host/local.ts")], {
  env,
  stdio: ["ignore", "pipe", "pipe"],
});
runner.stdout.on("data", (d) => process.stdout.write(`[core-old] ${d}`));
await new Promise((r) => setTimeout(r, 1500)); // let it acquire + claim the turn
runner.kill("SIGKILL");
await new Promise((r) => setTimeout(r, 300));
const mid = await req(
  "GET",
  `/internal/core/personas/${personaId}/state`,
  ptoken,
);
assert(mid.json.running_turn, "expected a running turn after kill");

// Dead writer's lease expires after SUMI_LEASE_TTL_MS; retry --once until a
// new generation acquires, recovers, and finishes the orphaned turn.
log("  waiting for dead lease to expire…");
let took = false;
for (let i = 0; i < 20; i++) {
  await new Promise((r) => setTimeout(r, 1000));
  const retry = spawnSync(
    "node",
    [join(CORE_DIR, "src/host/local.ts"), "--once"],
    {
      env,
      encoding: "utf8",
      timeout: 90_000,
    },
  );
  if (retry.status === 0) {
    took = true;
    break;
  }
}
assert(took, "new writer never acquired the lease");
const evs3 = await req(
  "GET",
  `/internal/core/personas/${personaId}/events?after_seq=0`,
  ptoken,
);
const received = evs3.json.events.filter(
  (e) =>
    e.kind === "input_received" &&
    e.payload.input_id === sub3.json.input.input_id,
);
assert(
  received.length === 1,
  `input received ${received.length} times — expected exactly-once`,
);
const finalOut = await req(
  "GET",
  `/internal/core/personas/${personaId}/outbox?after_seq=0`,
  ptoken,
);
const replies = finalOut.json.outbox.filter(
  (o) => o.payload.input_id === sub3.json.input.input_id,
);
assert(
  replies.length === 1,
  `expected 1 reply for killed input, got ${replies.length}`,
);
log("  recovered: interrupted turn requeued, committed once");

svc.kill("SIGKILL");
log("PASS — all e2e scenarios green on real PG + real Go + real Node");
