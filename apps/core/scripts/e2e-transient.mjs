#!/usr/bin/env node
/**
 * E2E: local host survives a transient state-service outage (Review-A F1).
 *
 * A running secretary must NOT exit when the state API/PG goes away: it
 * retries with bounded backoff and keeps serving once the service returns.
 * Covers: baseline reply → SIGKILL state-dev → secretary stays alive and
 * logs paced retries → restart state-dev → new input completes through the
 * same long-lived process → SIGTERM exits promptly.
 *
 * Requires SUMI_TEST_DB_URL (or SUMI_DB_URL). Builds the state-dev binary.
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error("e2e-transient: SUMI_TEST_DB_URL required — real PostgreSQL");
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const PORT = 9390 + (process.pid % 500);
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const personaId = uuidv7();

function log(...a) {
  console.log("[e2e-t]", ...a);
}
function fail(msg) {
  console.error("[e2e-t] FAIL:", msg);
  process.exit(1);
}
function assert(cond, msg) {
  if (!cond) fail(msg);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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
    await sleep(200);
  }
  fail("state-dev did not become healthy");
}

const binDir = mkdtempSync(join(tmpdir(), "sumi-e2et-"));
const bin = join(binDir, "state-dev");
log("building state-dev…");
const build = spawnSync(
  "go",
  ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"],
  { cwd: API_DIR, stdio: "inherit" },
);
if (build.status !== 0) fail("go build failed");

let svc = null;
function startSvc() {
  svc = spawn(bin, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: DB_URL,
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  svc.stderr.on("data", (d) => process.stderr.write(`[state-dev] ${d}`));
}
log("starting state-dev on", BASE);
startSvc();
process.on("exit", () => svc?.kill("SIGKILL"));
await waitHealth(Date.now() + 15_000);

const created = await req("POST", "/internal/core/personas", ADMIN, {
  persona_id: personaId,
  display_name: "transient e2e",
});
assert(created.status === 201, `createPersona ${created.status}`);
const ptoken = created.json.persona_token;

async function submit(text) {
  const sub = await req(
    "POST",
    `/internal/core/personas/${personaId}/inputs`,
    ptoken,
    {
      input_id: `in-${randomUUID()}`,
      kind: "message",
      payload: { text },
      actor_kind: "human",
      actor_id: "e2e",
      source_surface: "e2e",
    },
  );
  assert(sub.status === 201, `submitInput ${sub.status}: ${sub.text}`);
  return sub.json.input.input_id;
}

async function waitReply(inputId, deadlineMs) {
  while (Date.now() < deadlineMs) {
    const out = await req(
      "GET",
      `/internal/core/personas/${personaId}/outbox?after_seq=0`,
      ptoken,
    );
    const hit = out.json.outbox.find(
      (o) => o.payload.input_id === inputId && o.kind === "turn_completed",
    );
    if (hit) return hit;
    await sleep(300);
  }
  return null;
}

// --- baseline: running secretary serves an input -------------------------
log("starting long-running secretary (mock provider)");
const env = {
  ...process.env,
  SUMI_STATE_URL: BASE,
  SUMI_PERSONA_ID: personaId,
  SUMI_PERSONA_TOKEN: ptoken,
  SUMI_MODEL_PROVIDER: "mock",
  SUMI_LEASE_TTL_MS: "4000",
};
const core = spawn("node", [join(CORE_DIR, "src/host/local.ts")], {
  env,
  stdio: ["ignore", "pipe", "pipe"],
});
let coreOut = "";
let coreErr = "";
let coreExit = null;
core.stdout.on("data", (d) => {
  coreOut += d;
  process.stdout.write(`[core] ${d}`);
});
core.stderr.on("data", (d) => {
  coreErr += d;
  process.stderr.write(`[core!] ${d}`);
});
core.on("exit", (code) => {
  coreExit = code;
});
process.on("exit", () => core.kill("SIGKILL"));

const in1 = await submit("before the outage");
const r1 = await waitReply(in1, Date.now() + 20_000);
assert(r1, "baseline input never answered");
log("  baseline reply:", r1.payload.output.text);

// --- kill the state service mid-run --------------------------------------
log("killing state-dev mid-run — secretary must NOT exit");
svc.kill("SIGKILL");
await sleep(4_000);
assert(
  coreExit === null,
  `secretary exited during outage (code ${coreExit}) — F1 regression`,
);
assert(
  /state call failed|state service unavailable|lease renewal failed/.test(
    coreOut,
  ),
  `no transient-retry log observed during outage:\n${coreOut}${coreErr}`,
);
log("  secretary alive and logging paced retries during outage");

// --- restart the service; the same process must recover ------------------
log("restarting state-dev; the same secretary process must recover");
startSvc();
await waitHealth(Date.now() + 15_000);
const in2 = await submit("after the outage");
const r2 = await waitReply(in2, Date.now() + 45_000);
assert(
  r2,
  `post-outage input never answered by the same process; coreOut:\n${coreOut}`,
);
assert(coreExit === null, "secretary died during recovery");
log("  recovered reply:", r2.payload.output.text);

// --- cancellation still exits promptly ------------------------------------
core.kill("SIGTERM");
for (let i = 0; i < 50 && coreExit === null; i++) await sleep(100);
assert(
  coreExit === 0,
  `SIGTERM did not stop the secretary cleanly (exit ${coreExit})`,
);
log("  SIGTERM exited promptly with code 0");

svc.kill("SIGKILL");
log("PASS — transient state-API outage recovered in-process, cancellation intact");
