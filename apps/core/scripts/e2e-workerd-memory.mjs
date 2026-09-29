#!/usr/bin/env node
/**
 * Workerd memory-branch lifecycle e2e: REAL local workerd (wrangler dev)
 * running the SecretaryObject Durable Object → real Go state service → real
 * PostgreSQL, with the real workspace file service configured so the state
 * service advertises file.read/file.write. The model is a local scripted
 * OpenAI-compatible SSE endpoint: ordinary turns get a short reply, and
 * memory branches run the private-workspace protocol through
 * memoryAgentRound (memory-e2e-support.mjs) — read the frozen source, write
 * a draft, reread that version and the source, review, confirm. It proves
 * lifecycle and durability, not memory quality.
 *
 * Each scenario is its own persona. Inputs are sent, then one wake; the
 * fetch-started drain claims the sealed chunk at a model boundary but never
 * starts a branch, so every branch round runs in an alarm-invoked drain.
 *
 * Scenarios (SUMI_E2E_MEMORY_SCENARIOS, comma list; default all):
 *   delayed      the branch's write round answers only after
 *                SUMI_E2E_BRANCH_DELAY_MS (default 30000 — past the 25s
 *                fetch-drain turn budget). With no further wake, the branch
 *                completes on alarms; more exchanges then apply it and the
 *                next turn sees the time-anchored fragment in place.
 *   failure      the branch's first round gets HTTP 500: the branch pauses
 *                as provider_error with a retry time and no issue, nothing
 *                is invented in its transcript or shown to the conversation,
 *                and the alarm at that time resumes the same branch.
 *   interrupted  the branch's review round is held open and the whole
 *                wrangler/workerd process group is killed: the checkpointed
 *                draft and transcript survive; after a restart the branch
 *                continues at review from them and the killed worker's draft
 *                is the one prepared.
 *
 * Env:
 *   SUMI_TEST_DB_URL      disposable database for the state and file
 *                         services (required)
 *   SUMI_E2E_PORT_BASE    model/state/worker/inspector/filesvc ports =
 *                         base..+4
 *   SUMI_E2E_OUT          directory for logs and the DO persistence
 *   SUMI_E2E_PSQL         optional command prefix that runs one SQL string
 *                         against the same database and prints rows
 *                         unaligned (e.g. "psql <url> -Atc"); enables the
 *                         branch/chunk row assertions
 *
 *   node scripts/e2e-workerd-memory.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  chatCompletionsSSE,
  fromChatCompletions,
  memoryAgentRound,
  memoryBranchView,
  memoryPathsInWorkspace,
  startFileService,
} from "./memory-e2e-support.mjs";

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
// The fetch-drain turn budget (SUMI_DRAIN_TURN_BUDGET_MS default, not
// overridden here). A round that outlives it ran in an alarm drain.
const DRAIN_TURN_BUDGET_MS = 25_000;
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
let files = null;
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
  try {
    files?.proc.kill("SIGKILL");
  } catch {}
  killWorkerGroup();
  try {
    sse?.close();
  } catch {}
}
process.on("exit", cleanup);
process.on("SIGINT", () => process.exit(130));
process.on("SIGTERM", () => process.exit(143));
let passed = 0;
// wrangler dev watches the sources: an edit during the run reloads the
// worker, which restarts any in-flight branch round like a crash would.
const workerReloads = () =>
  readdirSync(OUT)
    .filter((f) => /^workerd-\d+\.log$/.test(f))
    .filter((f) =>
      readFileSync(join(OUT, f), "utf8").includes("Reloading local server"),
    );
const fail = (m, ev) => {
  const reloads = workerReloads();
  if (reloads.length)
    console.error(
      `[e2e-wm ${el()}] NOTE: wrangler reloaded the worker on a source change (${reloads.join(", ")}); this run does not test the lifecycle`,
    );
  console.error(`[e2e-wm ${el()}] FAIL:`, m);
  if (ev !== undefined) console.error(JSON.stringify(ev, null, 1));
  process.exit(1);
};
const assert = (c, m, ev) => {
  if (!c) fail(m, ev);
  passed++;
  log(`ok ${passed} - ${m}`);
};

// --- scripted model --------------------------------------------------------
// Inputs carry `scenario=<name>`, so a request's frozen prefix names its
// persona's scenario. Each scenario scripts its branch rounds per chunk and
// stage; turns get an immediate short reply. at/endedAt are monotonic
// milliseconds (performance.now): durations must not follow wall-clock
// adjustments.
const branchCalls = []; // { scenario, chunk, n, stage, start, at, endedAt, outcome, own }
const turnBodies = []; // { scenario, messages: [{ role, content }] }
const behaviors = new Map(); // scenario -> (chunk, stage, n) => behavior
let workerStarts = 0;
const scenarioOf = (text) =>
  /scenario=(delayed|failure|interrupted)\b/.exec(text)?.[1] ?? "?";
sse = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", async () => {
    let messages = [];
    try {
      messages = fromChatCompletions(JSON.parse(body).messages ?? []);
    } catch {}
    const view = memoryBranchView(messages);
    if (!view) {
      turnBodies.push({
        scenario: scenarioOf(body),
        messages: messages.map((m) => ({
          role: m.role,
          content: m.content.slice(0, 400),
        })),
      });
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(chatCompletionsSSE({ text: "ack", calls: [] }));
      return;
    }
    const scenario = scenarioOf(
      JSON.stringify(view.prefix.map((m) => m.content)),
    );
    const decision = memoryAgentRound(view, {
      label: `workerd start #${workerStarts}`,
    });
    const chunk = view.chunk;
    const n =
      branchCalls.filter((c) => c.scenario === scenario && c.chunk === chunk)
        .length + 1;
    const call = {
      scenario,
      chunk,
      n,
      stage: decision.stage,
      start: workerStarts,
      at: performance.now(),
      endedAt: null,
      outcome: null,
      own: view.own.length,
      writes: view.own.flatMap((m) =>
        (m.toolCalls ?? [])
          .filter((c) => c.name === "file.write")
          .map((c) => c.arguments.content_text.split("\n")[0]),
      ),
    };
    branchCalls.push(call);
    const b = behaviors.get(scenario)?.(chunk, decision.stage, n) ?? {
      kind: "answer",
      ms: 0,
    };
    log(
      `model: ${scenario} chunk ${chunk} call #${n} ${decision.stage} -> ${b.kind}${b.ms ? ` ${b.ms}ms` : ""}`,
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
      res.on("close", () => {
        held.delete(res);
        call.endedAt = performance.now();
      });
      return;
    }
    await sleep(b.ms);
    call.outcome = "answered";
    call.endedAt = performance.now();
    res.end(chatCompletionsSSE(decision));
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
files = await startFileService({ dbUrl: DB_URL, port: BASE + 4, outDir: OUT });
const svcOut = openSync(join(OUT, "state-dev.log"), "a");
svc = spawn(bin, [], {
  env: {
    ...process.env,
    ...files.env,
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
  if (created.status !== 201)
    fail(`createPersona ${created.status}`, created.text);
  personas[name] = { id, token: created.json.persona_token };
}
const first = Object.values(personas)[0];
const advertised =
  (
    await sreq(
      "GET",
      `/internal/core/personas/${first.id}/tools`,
      first.token,
    )
  ).json?.tools ?? [];
assert(
  advertised.includes("file.read") && advertised.includes("file.write"),
  "the state service advertises the workspace file tools",
  advertised,
);

// --- wrangler dev, in its own process group --------------------------------
const persistDir = join(OUT, "wrangler-state");
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
    payload: { text: `scenario=${name} ${text}` },
    actor_kind: "human",
    actor_id: "e2e-wm",
    source_surface: "workerd-memory-e2e",
  });
  if (r.status !== 201) fail(`submit ${r.status}`, r.text);
}
let wakes = 0;
const wake = async (name) => {
  wakes++;
  const r = await fetch(`${WORKER}/personas/${personas[name].id}/wake`, {
    method: "POST",
  });
  if (!r.ok) fail(`wake ${r.status}: ${await r.text()}`);
};
async function waitFor(what, fn, ms) {
  const until = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > until)
      fail(`timed out waiting for ${what}`, {
        calls: branchCalls.map(
          (c) => `${c.scenario} ${c.chunk} #${c.n} ${c.stage} ${c.outcome}`,
        ),
      });
    await sleep(500);
  }
}
/** Chunk and branch rows through SUMI_E2E_PSQL, or null without a hook. */
function branchRow(name, chunkSeq) {
  if (!PSQL) return null;
  const sql = `SELECT row_to_json(x) FROM (SELECT c.status AS chunk_status, c.replacement, b.status, b.revision, b.retry_at, b.issue, (b.state::jsonb)->>'failures' AS failures, (b.state::jsonb)->>'pause_reason' AS pause_reason, (b.state::jsonb)->>'rounds' AS rounds, (b.state::jsonb)->'candidate'->>'version' AS candidate_version, (b.state::jsonb)->'candidate'->>'text' AS candidate_text, (b.state::jsonb)->'in_flight' AS in_flight, (b.state::jsonb)->'final'->>'version' AS final_version, jsonb_array_length((b.state::jsonb)->'messages') AS transcript FROM core_memory_chunks c LEFT JOIN core_memory_branches b USING (persona_id, chunk_seq) WHERE c.persona_id = '${personas[name].id}' AND c.chunk_seq = ${chunkSeq}) x`;
  const r = spawnSync(PSQL[0], [...PSQL.slice(1), sql], { encoding: "utf8" });
  if (r.status !== 0) fail(`psql hook failed: ${r.stderr}`);
  return JSON.parse(r.stdout.trim() || "null");
}
function memoryStatusInputs(name) {
  if (!PSQL) return null;
  const sql = `SELECT count(*) FROM core_inputs WHERE persona_id = '${personas[name].id}' AND kind = 'memory_status'`;
  const r = spawnSync(PSQL[0], [...PSQL.slice(1), sql], { encoding: "utf8" });
  if (r.status !== 0) fail(`psql hook failed: ${r.stderr}`);
  return Number(r.stdout.trim());
}
const calls = (name, chunk) =>
  branchCalls.filter((c) => c.scenario === name && c.chunk === chunk);
const pad = "x".repeat(44 * 1024);
/** Two padded exchanges and one wake: chunk 1 seals, and the fetch drain's
 * model boundary claims it. */
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
const headerRe =
  /^\[Memory fragment recorded \d{4}-\d{2}-\d{2}T\S+ through \d{4}-\d{2}-\d{2}T\S+; journal seq (\d+) through (\d+)\.\]\n/;
const summary = { out: OUT, delay_ms: DELAY_MS, scenarios: {} };

await startWorker();

// --- delayed ---------------------------------------------------------------
if (personas.delayed) {
  const name = "delayed";
  log(`scenario delayed: the write round answers after ${DELAY_MS}ms`);
  assert(
    DELAY_MS > DRAIN_TURN_BUDGET_MS,
    `SUMI_E2E_BRANCH_DELAY_MS=${DELAY_MS} exceeds the ${DRAIN_TURN_BUDGET_MS}ms fetch-drain budget`,
  );
  behaviors.set(name, (chunk, stage) =>
    chunk === 1 && stage === "write"
      ? { kind: "answer", ms: DELAY_MS }
      : { kind: "answer", ms: 0 },
  );
  const wakesBefore = wakes;
  await sealChunkOne(name);
  await waitFor(
    "delayed: chunk 1 prepared",
    async () => (await mem(name))?.prepared >= 1,
    DELAY_MS + 120_000,
  );
  const c1 = calls(name, 1);
  const delayed = c1.find((c) => c.stage === "write");
  const stages = c1.map((c) => c.stage);
  // The semantic requirement is that the write round outlived the
  // fetch-drain turn budget and the branch still went on to finish: only
  // an alarm-invoked drain may run a branch or hold one that long, and no
  // wake was sent after the inputs. The scripted sleep may land a few ms
  // short of DELAY_MS (observed 29999.75ms), so the assertion is the drain
  // boundary, not the nominal sleep length.
  assert(
    delayed?.outcome === "answered" &&
      delayed.endedAt - delayed.at >= DRAIN_TURN_BUDGET_MS &&
      c1.every((c) => c.outcome === "answered"),
    "the delayed write round outlived the fetch budget and was answered, not interrupted",
    c1,
  );
  assert(
    wakes === wakesBefore + 1 &&
      stages.at(-1) === "confirm" &&
      stages.indexOf("review") > stages.indexOf("write") &&
      c1.filter((c) => c.stage === "write").length === 1,
    "after the single wake the branch read, wrote once, reread, reviewed and confirmed on alarms",
    { wakes: wakes - wakesBefore, stages },
  );
  const row = branchRow(name, 1);
  if (row)
    assert(
      row.chunk_status === "prepared" &&
        row.status === "prepared" &&
        row.final_version === "1" &&
        row.in_flight === null &&
        Number(row.failures) === 0,
      "chunk 1 and its branch rows are prepared from version 1 with no failure",
      row,
    );
  log(
    `  prepared; write round took ${((delayed.endedAt - delayed.at) / 1000).toFixed(1)}s`,
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
  const seen = turnBodies.find(
    (t) =>
      t.scenario === name &&
      t.messages.at(-1)?.content.includes("which color do I like now?") &&
      t.messages.some(
        (m) =>
          headerRe.test(m.content) && m.content.includes("L1 chunk 1 ("),
      ),
  );
  assert(
    seen,
    "the next turn receives the applied, time-anchored fragment",
    turnBodies
      .filter((t) => t.scenario === name)
      .map((t) => t.messages.map((m) => m.content.slice(0, 60))),
  );
  // Fragments sit where their journal ranges were, in journal order, and
  // what no fragment covers stays raw after them: here the final question.
  const frags = seen.messages
    .map((m, i) => ({ i, m: headerRe.exec(m.content) }))
    .filter((x) => x.m)
    .map((x) => ({ i: x.i, first: Number(x.m[1]) }));
  assert(
    frags[0].first === 1 &&
      frags[0].i === 1 &&
      frags.every((f, k) => k === 0 || f.first > frags[k - 1].first) &&
      !seen.messages.some((m) =>
        /^(?:\[Received [^\]]*\]\n)?\[human\] scenario=delayed COLOR=amber/.test(m.content),
      ) &&
      seen.messages.at(-1).content.includes("which color do I like now?"),
    "chunk 1's fragment renders first, at its original position, and later messages follow it",
    { frags, messages: seen.messages.map((m) => m.content.slice(0, 160)) },
  );
  const header = seen.messages[frags[0].i].content.split("\n")[0];
  const applied = branchRow(name, 1);
  if (applied)
    assert(applied.chunk_status === "applied", "chunk 1 applied", applied);
  log(`  applied; turn saw: ${header}`);
  summary.scenarios.delayed = {
    write_round_ms: delayed.endedAt - delayed.at,
    stages,
    prepared_row: row,
    applied_status: applied?.chunk_status,
    header,
    status: await mem(name),
  };
}

// --- failure ---------------------------------------------------------------
if (personas.failure) {
  const name = "failure";
  log("scenario failure: the branch's first round gets HTTP 500");
  behaviors.set(name, (chunk, _stage, n) =>
    chunk === 1 && n === 1 ? { kind: "http500" } : { kind: "answer", ms: 0 },
  );
  await sealChunkOne(name);
  await waitFor(
    "failure: the first branch call",
    async () => calls(name, 1)[0]?.outcome === "http500",
    60_000,
  );
  const paused = await waitFor(
    "failure: branch paused",
    async () => {
      const b = (await mem(name))?.branches?.find((x) => x.chunk_seq === 1);
      return b?.status === "paused" ? b : null;
    },
    30_000,
  );
  assert(
    paused.pause_reason === "provider_error" &&
      paused.retry_at !== null &&
      paused.issue === null,
    "a single upstream failure pauses the branch with a retry time and no issue",
    paused,
  );
  const pausedRow = branchRow(name, 1);
  if (pausedRow)
    assert(
      pausedRow.chunk_status === "preparing" &&
        pausedRow.status === "paused" &&
        Number(pausedRow.failures) === 1 &&
        pausedRow.in_flight === null &&
        pausedRow.candidate_version === null &&
        memoryStatusInputs(name) === 0,
      "the rows record one failure, no draft and no memory_status input",
      { pausedRow, memory_status_inputs: memoryStatusInputs(name) },
    );
  // A turn during the pause: the original stays, and no mechanism status
  // reaches the conversation for an ordinary retry.
  await say(name, "while the branch waits");
  await wake(name);
  await waitFor(
    "failure: reply during the pause",
    async () => (await outboxCount(name)) >= 3,
    60_000,
  );
  const during = turnBodies
    .filter((t) => t.scenario === name)
    .find((t) => t.messages.at(-1)?.content.includes("while the branch waits"));
  assert(
    during.messages.some((m) =>
      /^(?:\[Received [^\]]*\]\n)?\[human\] scenario=failure COLOR=amber/.test(m.content),
    ) &&
      !during.messages.some(
        (m) =>
          m.content.startsWith("[Current memory mechanism status") ||
          m.content.startsWith("[Memory fragment"),
      ),
    "the paused branch leaves the original in context and adds no status or fragment",
    during.messages.map((m) => m.content.slice(0, 60)),
  );
  await waitFor(
    "failure: chunk 1 prepared",
    async () => (await mem(name))?.prepared >= 1,
    150_000,
  );
  const c1 = calls(name, 1);
  const retryWaitMs = c1[1].at - c1[0].at;
  assert(
    c1[0].outcome === "http500" &&
      c1[1].stage === "read_source" &&
      c1[1].own === c1[0].own &&
      c1.slice(1).every((c) => c.outcome === "answered") &&
      c1.at(-1).stage === "confirm",
    "the alarm resumed the same branch from its unchanged transcript and completed it",
    c1,
  );
  const retryAt = Date.parse(paused.retry_at);
  assert(
    retryWaitMs >= 25_000,
    "the retry waited for the recorded retry time",
    { retryWaitMs, retry_at: paused.retry_at, retryAt },
  );
  const row = branchRow(name, 1);
  if (row)
    assert(
      row.chunk_status === "prepared" &&
        row.status === "prepared" &&
        Number(row.failures) === 0 &&
        row.issue === null,
      "the completed branch clears its failure count",
      row,
    );
  log(`  retried after ${(retryWaitMs / 1000).toFixed(1)}s`);
  summary.scenarios.failure = {
    paused,
    paused_row: pausedRow,
    retry_wait_ms: retryWaitMs,
    stages: c1.map((c) => `${c.stage}:${c.outcome}`),
    prepared_row: row,
    status: await mem(name),
  };
}

// --- interrupted -----------------------------------------------------------
if (personas.interrupted) {
  const name = "interrupted";
  log("scenario interrupted: host killed while the review round is held");
  behaviors.set(name, (chunk, stage) =>
    chunk === 1 && stage === "review" && workerStarts === 1
      ? { kind: "hold" }
      : { kind: "answer", ms: 0 },
  );
  await sealChunkOne(name);
  await waitFor(
    "interrupted: review round held",
    async () => calls(name, 1).find((c) => c.stage === "review"),
    90_000,
  );
  await sleep(1_000);
  const before = branchRow(name, 1);
  if (before)
    assert(
      before.chunk_status === "preparing" &&
        before.status === "running" &&
        before.candidate_version === "1" &&
        before.candidate_text.includes("draft by workerd start #1") &&
        before.in_flight !== null,
      "the draft and the admitted review round are checkpointed before the kill",
      before,
    );
  const pgid = worker.pid;
  const members = groupMembers(pgid);
  assert(
    members.some((l) => /workerd/.test(l)),
    "workerd is in the spawned process group",
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
  const st = await mem(name);
  assert(
    st.preparing === 1 && st.prepared === 0 && st.failed === 0,
    "the chunk stays preparing across the kill",
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
  const afterRestart = c1.filter((c) => c.start === 2);
  assert(
    JSON.stringify(afterRestart.map((c) => c.stage)) ===
      JSON.stringify(["review", "confirm"]) &&
      afterRestart[0].writes.length === 1 &&
      afterRestart[0].writes[0].includes("draft by workerd start #1"),
    "after the restart the branch continued at review from the killed worker's transcript and draft",
    c1,
  );
  const row = branchRow(name, 1);
  if (row)
    assert(
      row.chunk_status === "prepared" &&
        row.final_version === "1" &&
        row.replacement?.includes("draft by workerd start #1"),
      "the prepared replacement is the draft written before the kill",
      row,
    );
  log("  prepared after restart from the durable draft");
  summary.scenarios.interrupted = {
    killed_group: members,
    before_kill: before,
    stages: c1.map((c) => `start#${c.start} ${c.stage}:${c.outcome}`),
    prepared_row: row,
    status: await mem(name),
  };
}

assert(
  memoryPathsInWorkspace(files.root).length === 0,
  "branch file operations never reached the workspace file service",
  memoryPathsInWorkspace(files.root),
);
assert(
  workerReloads().length === 0,
  "wrangler never reloaded the worker: every restart was the test's own",
);
summary.branch_calls = branchCalls;
summary.row_assertions = PSQL !== null;
summary.checks = passed;
writeFileSync(join(OUT, "summary.json"), JSON.stringify(summary, null, 2));
log(
  `PASS — ${passed} checks: ${SCENARIOS.join(", ")}${PSQL ? " (with branch-row assertions)" : " (status-level assertions only)"}; summary in ${OUT}`,
);
process.exit(0);
