#!/usr/bin/env node
/**
 * Workerd memory-preparation lifecycle e2e: REAL local workerd (wrangler
 * dev) running the SecretaryObject Durable Object → real Go state service →
 * real PostgreSQL. The model is a local scripted OpenAI-compatible SSE
 * endpoint: it proves lifecycle and durability, not memory quality.
 *
 * Scenarios (SUMI_E2E_MEMORY_SCENARIOS, comma list; default all):
 *   delayed      the branch answers only after SUMI_E2E_BRANCH_DELAY_MS
 *                (default 30000 — past the 25s fetch-drain turn budget). The
 *                answer is shelved from that one call; more exchanges then
 *                apply it and the next turn sees the time-anchored fragment.
 *   failure      the first branch call gets HTTP 500: one recorded attempt,
 *                retried on the alarm, then prepared.
 *   interrupted  the branch is held open and the whole wrangler/workerd
 *                process group is killed mid-branch: nothing is recorded;
 *                after a restart the claim counts one interruption (not an
 *                attempt) and the chunk is prepared.
 *
 * Env:
 *   SUMI_TEST_DB_URL      database for the state service (required)
 *   SUMI_E2E_PORT_BASE    model/state/worker/inspector ports = base..+3
 *   SUMI_E2E_OUT          directory for logs and the DO persistence
 *   SUMI_E2E_PSQL         optional command prefix that runs one SQL string
 *                         against the same database and prints rows
 *                         unaligned (e.g. "psql <url> -Atc"); enables the
 *                         chunk-row assertions (attempts/interruptions)
 *
 *   node scripts/e2e-workerd-memory.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, openSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error("e2e-workerd-memory: SUMI_TEST_DB_URL required");
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const BASE = Number(
  process.env.SUMI_E2E_PORT_BASE ?? 8960 + (process.pid % 90),
);
const SSE = `http://127.0.0.1:${BASE}`;
const STATE = `http://127.0.0.1:${BASE + 1}`;
const WORKER = `http://127.0.0.1:${BASE + 2}`;
const OUT = process.env.SUMI_E2E_OUT
  ? resolve(process.env.SUMI_E2E_OUT)
  : mkdtempSync(join(tmpdir(), "sumi-e2ewm-"));
mkdirSync(OUT, { recursive: true });
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;
const DELAY_MS = Number(process.env.SUMI_E2E_BRANCH_DELAY_MS ?? 30_000);
const SCENARIOS = (
  process.env.SUMI_E2E_MEMORY_SCENARIOS ?? "delayed,failure,interrupted"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const PSQL = process.env.SUMI_E2E_PSQL?.trim().split(/\s+/) ?? null;

const t0 = performance.now();
const el = () => `${((performance.now() - t0) / 1000).toFixed(1)}s`;
const log = (...a) => console.log(`[e2e-wm ${el()}]`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let svc = null;
let worker = null;
let sse = null;
const held = new Set();
function killWorkerGroup(sig = "SIGKILL") {
  if (!worker) return;
  try {
    process.kill(-worker.pid, sig);
  } catch {}
}
function cleanup() {
  for (const res of held) {
    try {
      res.destroy();
    } catch {}
  }
  try {
    svc?.kill("SIGKILL");
  } catch {}
  killWorkerGroup();
  try {
    sse?.close();
  } catch {}
}
process.on("exit", cleanup);
process.on("SIGINT", () => process.exit(130));
process.on("SIGTERM", () => process.exit(143));
const fail = (m, ev) => {
  console.error(`[e2e-wm ${el()}] FAIL:`, m);
  if (ev !== undefined) console.error(JSON.stringify(ev, null, 1));
  process.exit(1);
};
const assert = (c, m, ev) => {
  if (!c) fail(m, ev);
};

// --- scripted model --------------------------------------------------------
// A request whose last message carries compact_target is a memory branch;
// its target's input ids name the scenario. Each scenario scripts its
// branch calls per chunk; turns get an immediate short reply.
// at/endedAt are monotonic milliseconds (performance.now): durations must not
// follow wall-clock adjustments.
const branchCalls = []; // { scenario, chunk, n, at, endedAt, outcome }
const turnBodies = []; // { scenario, messages }
const behaviors = new Map(); // scenario -> (chunk, n) => behavior
const scenarioOf = (text) =>
  /\bin-(delayed|failure|interrupted)-/.exec(text)?.[1] ?? "?";
function finish(res, text) {
  res.end(
    `data: ${JSON.stringify({ choices: [{ delta: { content: text } }] })}\n\n` +
      `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n` +
      "data: [DONE]\n\n",
  );
}
sse = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", async () => {
    let messages = [];
    try {
      messages = JSON.parse(body).messages ?? [];
    } catch {}
    const last = String(messages[messages.length - 1]?.content ?? "");
    const at = last.indexOf("compact_target\n");
    if (at < 0) {
      turnBodies.push({ scenario: scenarioOf(body), messages });
      res.writeHead(200, { "content-type": "text/event-stream" });
      finish(res, "ack");
      return;
    }
    const target = JSON.parse(last.slice(at + "compact_target\n".length));
    const scenario = scenarioOf(JSON.stringify(target.events));
    const chunk = target.chunk_seq;
    const n =
      branchCalls.filter((c) => c.scenario === scenario && c.chunk === chunk)
        .length + 1;
    const call = {
      scenario,
      chunk,
      n,
      at: performance.now(),
      endedAt: null,
      outcome: null,
    };
    branchCalls.push(call);
    const b = behaviors.get(scenario)?.(chunk, n) ?? { kind: "answer", ms: 0 };
    log(
      `model: ${scenario} branch chunk ${chunk} call #${n} -> ${b.kind}${b.ms ? ` ${b.ms}ms` : ""}`,
    );
    if (b.kind === "http500") {
      call.outcome = "http500";
      call.endedAt = performance.now();
      res.writeHead(500, { "content-type": "application/json" });
      res.end('{"error":{"message":"scripted upstream failure"}}');
      return;
    }
    res.writeHead(200, { "content-type": "text/event-stream" });
    if (b.kind === "hold") {
      held.add(res);
      call.outcome = "held";
      res.write(`data: {"choices":[{"delta":{"content":"partial "}}]}\n\n`);
      res.on("close", () => {
        held.delete(res);
        call.endedAt = performance.now();
      });
      return;
    }
    await sleep(b.ms);
    call.outcome = "answered";
    call.endedAt = performance.now();
    finish(res, `organized memory of ${scenario} chunk ${chunk}`);
  });
});
await new Promise((r) => sse.listen(BASE, "127.0.0.1", r));
log("scripted model on", SSE);

// --- state service ---------------------------------------------------------
async function sreq(method, path, token, body) {
  const res = await fetch(STATE + path, {
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
  } catch {}
  return { status: res.status, json, text };
}
async function waitOk(url, until) {
  while (Date.now() < until) {
    try {
      if ((await fetch(url)).ok) return;
    } catch {}
    await sleep(250);
  }
  fail(`${url} never became ready`);
}
const bin = join(OUT, "state-dev");
log("building state-dev…");
if (
  spawnSync("go", ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"], {
    cwd: API_DIR,
    stdio: "inherit",
  }).status !== 0
)
  fail("go build failed");
const svcOut = openSync(join(OUT, "state-dev.log"), "a");
svc = spawn(bin, [], {
  env: {
    ...process.env,
    SUMI_DB_URL: DB_URL,
    SUMI_CORE_STATE_TOKEN: ADMIN,
    SUMI_STATE_LISTEN: `127.0.0.1:${BASE + 1}`,
  },
  stdio: ["ignore", svcOut, svcOut],
});
await waitOk(`${STATE}/health`, Date.now() + 30_000);
log("state-dev up on", STATE);

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const personas = {};
for (const name of SCENARIOS) {
  const id = uuidv7();
  const created = await sreq("POST", "/internal/core/personas", ADMIN, {
    persona_id: id,
    display_name: `workerd memory e2e ${name}`,
  });
  assert(
    created.status === 201,
    `createPersona ${created.status}`,
    created.text,
  );
  personas[name] = { id, token: created.json.persona_token };
}

// --- wrangler dev, in its own process group --------------------------------
const persistDir = join(OUT, "wrangler-state");
let workerStarts = 0;
async function startWorker() {
  workerStarts++;
  const out = openSync(join(OUT, `workerd-${workerStarts}.log`), "a");
  const vars = [
    `SUMI_STATE_URL:${STATE}`,
    "SUMI_MODEL_PROVIDER:openai",
    `SUMI_MODEL_BASE_URL:${SSE}`,
    "SUMI_MODEL_API_KEY:e2e",
    "SUMI_MODEL_MODEL:e2e-scripted",
    `SUMI_MODEL_TIMEOUT_MS:${Math.max(120_000, DELAY_MS + 60_000)}`,
    ...Object.values(personas).map(
      (p) => `SUMI_PERSONA_TOKEN_${p.id.replaceAll("-", "_")}:${p.token}`,
    ),
  ];
  worker = spawn(
    "pnpm",
    [
      "exec",
      "wrangler",
      "dev",
      "--config",
      "wrangler.jsonc",
      "--port",
      String(BASE + 2),
      "--ip",
      "127.0.0.1",
      "--inspector-port",
      String(BASE + 3),
      "--persist-to",
      persistDir,
      ...vars.flatMap((v) => ["--var", v]),
      "--show-interactive-dev-session=false",
    ],
    {
      cwd: CORE_DIR,
      env: { ...process.env, CI: "1" },
      stdio: ["ignore", out, out],
      detached: true,
    },
  );
  await waitOk(`${WORKER}/health`, Date.now() + 90_000);
  log(`workerd up (start #${workerStarts}, process group ${worker.pid})`);
}
function groupMembers(pgid) {
  const r = spawnSync("ps", ["-o", "pid=,pgid=,args=", "-g", String(pgid)], {
    encoding: "utf8",
  });
  return (
    r.stdout
      .split("\n")
      .map((l) => l.trim())
      .filter(Boolean)
      .filter((l) => l.split(/\s+/)[1] === String(pgid))
      // Process arguments carry persona tokens as --var values; keep them out
      // of logs and the summary.
      .map((l) =>
        l.replace(/(SUMI_PERSONA_TOKEN_[0-9a-f_]+:)\S+/g, "$1<redacted>"),
      )
  );
}

const P = (name) => `/internal/core/personas/${personas[name].id}`;
const mem = async (name) =>
  (await sreq("GET", `${P(name)}/memory`, personas[name].token)).json;
async function outboxCount(name) {
  const r = await sreq(
    "GET",
    `${P(name)}/outbox?after_seq=0`,
    personas[name].token,
  );
  return (r.json?.outbox ?? []).length;
}
async function say(name, text) {
  const r = await sreq("POST", `${P(name)}/inputs`, personas[name].token, {
    input_id: `in-${name}-${randomUUID()}`,
    kind: "message",
    payload: { text },
    actor_kind: "human",
    actor_id: "e2e-wm",
    source_surface: "workerd-memory-e2e",
  });
  assert(r.status === 201, `submit ${r.status}`, r.text);
}
const wake = async (name) => {
  const r = await fetch(`${WORKER}/personas/${personas[name].id}/wake`, {
    method: "POST",
  });
  assert(r.ok, `wake ${r.status}: ${await r.text()}`);
};
async function waitFor(what, fn, ms) {
  const until = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > until) fail(`timed out waiting for ${what}`);
    await sleep(500);
  }
}
/** Chunk rows through SUMI_E2E_PSQL, or null when no hook is configured. */
function chunkRow(name, chunkSeq) {
  if (!PSQL) return null;
  const sql = `SELECT status, attempts, interruptions, COALESCE(last_error, '') FROM core_memory_chunks WHERE persona_id = '${personas[name].id}' AND chunk_seq = ${chunkSeq}`;
  const r = spawnSync(PSQL[0], [...PSQL.slice(1), sql], { encoding: "utf8" });
  assert(r.status === 0, `psql hook failed: ${r.stderr}`);
  const [status, attempts, interruptions, lastError] = r.stdout
    .trim()
    .split("|");
  return {
    status,
    attempts: Number(attempts),
    interruptions: Number(interruptions),
    last_error: lastError,
  };
}
const calls = (name, chunk) =>
  branchCalls.filter((c) => c.scenario === name && c.chunk === chunk);
const pad = "x".repeat(44 * 1024);
async function sealChunkOne(name) {
  await say(name, `COLOR=amber ${pad}`);
  await say(name, `second topic ${pad}`);
  await wake(name);
  await waitFor(
    `${name}: two replies`,
    async () => (await outboxCount(name)) >= 2,
    60_000,
  );
}
const summary = { out: OUT, delay_ms: DELAY_MS, scenarios: {} };

await startWorker();

// --- delayed ---------------------------------------------------------------
if (personas.delayed) {
  const name = "delayed";
  log(`scenario delayed: branch answers after ${DELAY_MS}ms`);
  behaviors.set(name, (chunk, n) =>
    chunk === 1 && n === 1
      ? { kind: "answer", ms: DELAY_MS }
      : { kind: "answer", ms: 0 },
  );
  await sealChunkOne(name);
  const first = await waitFor(
    "delayed: branch call",
    async () => calls(name, 1)[0],
    30_000,
  );
  await waitFor(
    "delayed: chunk 1 prepared",
    async () => (await mem(name))?.prepared >= 1,
    DELAY_MS + 90_000,
  );
  const c1 = calls(name, 1);
  assert(
    c1.length === 1,
    `chunk 1 must be prepared from its single branch call, got ${c1.length}`,
    branchCalls,
  );
  // The semantic requirement is that the call outlived the fetch-drain
  // turn budget (SUMI_DRAIN_TURN_BUDGET_MS default 25s, not overridden
  // here): that is what makes this answer "the delayed one" — the drain
  // yielded mid-call and the branch completed on a later alarm. The
  // scripted sleep itself may land a few ms short of DELAY_MS (observed
  // 29999.75ms in CI), so the assertion is the drain boundary, not the
  // nominal sleep length.
  const DRAIN_TURN_BUDGET_MS = 25_000;
  assert(
    DELAY_MS > DRAIN_TURN_BUDGET_MS,
    `SUMI_E2E_BRANCH_DELAY_MS=${DELAY_MS} must exceed the ${DRAIN_TURN_BUDGET_MS}ms drain budget to be meaningful`,
  );
  assert(
    first.outcome === "answered" &&
      first.endedAt - first.at >= DRAIN_TURN_BUDGET_MS,
    "the branch response was not the delayed one",
    first,
  );
  const st = await mem(name);
  assert(st.failed === 0, "no chunk may fail", st);
  const row = chunkRow(name, 1);
  if (row)
    assert(
      row.status === "prepared" &&
        row.attempts === 0 &&
        row.interruptions === 0,
      "chunk 1 row",
      row,
    );
  log(
    `  prepared after ${((first.endedAt - first.at) / 1000).toFixed(1)}s in one call; row=${JSON.stringify(row)}`,
  );

  await say(name, `third ${pad}`);
  await say(name, `fourth ${pad}`);
  await say(name, "which color do I like now?");
  await wake(name);
  await waitFor(
    "delayed: five replies",
    async () => (await outboxCount(name)) >= 5,
    90_000,
  );
  const headerRe =
    /^\[Memory fragment recorded \d{4}-\d{2}-\d{2}T\S+ through \d{4}-\d{2}-\d{2}T\S+; journal seq \d+ through \d+\.\]\n/;
  // Turn requests carry no input ids; the question and the fragment text
  // are unique to this scenario.
  const seen = turnBodies.find(
    (t) =>
      t.messages.some((m) =>
        String(m.content).includes("which color do I like now?"),
      ) &&
      t.messages.some(
        (m) =>
          headerRe.test(String(m.content)) &&
          String(m.content).includes("organized memory of delayed chunk 1"),
      ),
  );
  assert(
    seen,
    "the next turn did not receive the applied, time-anchored fragment",
  );
  const header = seen.messages
    .map((m) => String(m.content))
    .find((c) => headerRe.test(c))
    .split("\n")[0];
  const applied = chunkRow(name, 1);
  if (applied) assert(applied.status === "applied", "chunk 1 applied", applied);
  log(`  applied; turn saw: ${header}`);
  summary.scenarios.delayed = {
    first_call_ms: first.endedAt - first.at,
    chunk1: row,
    applied,
    header,
    status: await mem(name),
  };
}

// --- failure ---------------------------------------------------------------
if (personas.failure) {
  const name = "failure";
  log("scenario failure: first branch call gets HTTP 500");
  behaviors.set(name, (chunk, n) =>
    chunk === 1 && n === 1
      ? { kind: "http500" }
      : { kind: "answer", ms: 2_000 },
  );
  await sealChunkOne(name);
  await waitFor(
    "failure: chunk 1 prepared",
    async () => (await mem(name))?.prepared >= 1,
    120_000,
  );
  const c1 = calls(name, 1);
  assert(
    c1.length === 2 &&
      c1[0].outcome === "http500" &&
      c1[1].outcome === "answered",
    "one failed call then one retry",
    c1,
  );
  const row = chunkRow(name, 1);
  if (row)
    assert(
      row.status === "prepared" &&
        row.attempts === 1 &&
        row.interruptions === 0 &&
        row.last_error.includes("500"),
      "chunk 1 row after a recorded failure",
      row,
    );
  log(
    `  retried after ${((c1[1].at - c1[0].at) / 1000).toFixed(1)}s; row=${JSON.stringify(row)}`,
  );
  summary.scenarios.failure = {
    retry_after_ms: c1[1].at - c1[0].at,
    chunk1: row,
    status: await mem(name),
  };
}

// --- interrupted -----------------------------------------------------------
if (personas.interrupted) {
  const name = "interrupted";
  log("scenario interrupted: host killed mid-branch");
  behaviors.set(name, (chunk, n) =>
    chunk === 1 && n === 1 ? { kind: "hold" } : { kind: "answer", ms: 2_000 },
  );
  await sealChunkOne(name);
  await waitFor(
    "interrupted: branch call",
    async () => calls(name, 1)[0],
    30_000,
  );
  await sleep(1_000);
  const before = chunkRow(name, 1);
  if (before)
    assert(
      before.status === "preparing" && before.attempts === 0,
      "claimed before the kill",
      before,
    );
  const pgid = worker.pid;
  const members = groupMembers(pgid);
  assert(
    members.some((l) => /workerd/.test(l)),
    "workerd is not in the spawned process group",
    members,
  );
  log(`  killing process group ${pgid}:\n    ${members.join("\n    ")}`);
  killWorkerGroup("SIGKILL");
  await waitFor(
    "process group gone",
    async () => groupMembers(pgid).length === 0,
    15_000,
  );
  await waitFor(
    "worker port closed",
    async () => {
      try {
        await fetch(`${WORKER}/health`);
        return false;
      } catch {
        return true;
      }
    },
    15_000,
  );
  const afterKill = chunkRow(name, 1);
  if (afterKill)
    assert(
      afterKill.status === "preparing" &&
        afterKill.attempts === 0 &&
        afterKill.interruptions === 0,
      "a killed host records nothing",
      afterKill,
    );
  const st = await mem(name);
  assert(
    st.preparing === 1 && st.failed === 0,
    "chunk left preparing by the kill",
    st,
  );
  await startWorker();
  await wake(name);
  await waitFor(
    "interrupted: chunk 1 prepared",
    async () => (await mem(name))?.prepared >= 1,
    120_000,
  );
  const c1 = calls(name, 1);
  assert(
    c1.length === 2 && c1[1].outcome === "answered",
    "one interrupted call then one completed call",
    c1,
  );
  const row = chunkRow(name, 1);
  if (row)
    assert(
      row.status === "prepared" &&
        row.attempts === 0 &&
        row.interruptions === 1,
      "chunk 1 row after an interruption",
      row,
    );
  log(`  prepared after restart; row=${JSON.stringify(row)}`);
  summary.scenarios.interrupted = {
    killed_group: members,
    before,
    after_kill: afterKill,
    chunk1: row,
    status: await mem(name),
  };
}

summary.branch_calls = branchCalls;
summary.row_assertions = PSQL !== null;
writeFileSync(join(OUT, "summary.json"), JSON.stringify(summary, null, 2));
log(
  `PASS — ${SCENARIOS.join(", ")}${PSQL ? " (with chunk-row assertions)" : " (status-level assertions only)"}; summary in ${OUT}`,
);
process.exit(0);
