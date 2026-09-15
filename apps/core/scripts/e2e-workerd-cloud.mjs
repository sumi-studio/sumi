#!/usr/bin/env node
/**
 * Cloud-shaped route e2e, run LOCALLY: real workerd (wrangler dev) running
 * the SecretaryObject Durable Object with the `alpha` environment's
 * bindings and vars → real Go state service → real PostgreSQL. This is not
 * a Cloudflare deployment; it proves the configuration and contracts the
 * deployment depends on.
 *
 * Taken from wrangler.jsonc env.alpha as-is: Durable Object binding,
 * migrations, SUMI_MODEL_PROVIDER=none, no dev wake flag, the SUMI_STATE
 * binding name. Local substitutions, each named in the summary:
 *   - SUMI_STATE is a service binding to a stand-in Worker that forwards to
 *     the loopback state service, ignoring the URL host as a VPC Service
 *     does; SUMI_STATE_URL is an unresolvable *.invalid host, so a state
 *     call that bypassed the binding would fail instead of succeeding.
 *   - The persona's selected connection points at a loopback scripted
 *     OpenAI-compatible endpoint (global_fetch_private_origin added for it).
 *   - Secrets are --var values; the wake URL is loopback http.
 *
 * The Go side is the same code the API mounts: runtime credential
 * (SUMI_CORE_RUNTIME_TOKEN) and the wake sweep (SUMI_CORE_WAKE_URL/TOKEN).
 * No test step calls the wake route for work: every reply below is woken
 * by the Go sweep or the Durable Object alarm.
 *
 * Scenarios: route authentication; ordinary conversation; workerd killed
 * mid-turn; workerd down when a message is admitted; state service killed
 * mid-turn (and /health/state reporting it); an unselected persona failing
 * visibly; a parked approval resuming after the human decision.
 *
 * Env:
 *   SUMI_TEST_DB_URL    admin URL of a PostgreSQL server; a fresh database
 *                       sumi_cloud_e2e_<time> is created on it (required)
 *   SUMI_E2E_PORT_BASE  model/state/worker/inspector = base..+3 (default 11871)
 *   SUMI_E2E_OUT        logs, generated configs, summary.json
 */
import { spawn, spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const ADMIN_DB = process.env.SUMI_TEST_DB_URL;
if (!ADMIN_DB) {
  console.error("e2e-workerd-cloud: SUMI_TEST_DB_URL required");
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const BASE = Number(process.env.SUMI_E2E_PORT_BASE ?? 11871);
const MODEL = `http://127.0.0.1:${BASE}`;
const STATE = `http://127.0.0.1:${BASE + 1}`;
const WORKER = `http://127.0.0.1:${BASE + 2}`;
const OUT = process.env.SUMI_E2E_OUT
  ? resolve(process.env.SUMI_E2E_OUT)
  : mkdtempSync(join(tmpdir(), "sumi-e2e-cloud-"));
mkdirSync(OUT, { recursive: true });
const dbUrl = new URL(ADMIN_DB);
dbUrl.pathname = `/sumi_cloud_e2e_${Date.now()}`;
const DB_URL = dbUrl.toString();

const secret = (p) => `${p}-${randomBytes(24).toString("hex")}`;
const ADMIN = secret("admin");
const RUNTIME = secret("runtime");
const WAKE = secret("wake");
const CONN_KEY = randomBytes(32).toString("base64");

const t0 = performance.now();
const el = () => `${((performance.now() - t0) / 1000).toFixed(1)}s`;
const log = (...a) => console.log(`[e2e-cloud ${el()}]`, ...a);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const summary = {
  note: "local workerd with the alpha environment's bindings; not a Cloudflare deployment",
  out: OUT,
  substitutions: [
    "SUMI_STATE: service binding to a loopback stand-in instead of the VPC Service",
    "SUMI_STATE_URL: http://sumi-state.invalid:8080 (unresolvable; binding-only)",
    "selected model connection: loopback scripted OpenAI-compatible SSE (global_fetch_private_origin)",
    "secrets passed as --var; wake URL is loopback http",
  ],
  scenarios: {},
};

let svc = null;
let worker = null;
let model = null;
const held = new Map(); // marker -> { res, release }
function killWorkerGroup(sig = "SIGKILL") {
  if (!worker) return;
  try {
    process.kill(-worker.pid, sig);
  } catch {}
}
function cleanup() {
  for (const h of held.values()) {
    try {
      h.res.destroy();
    } catch {}
  }
  try {
    svc?.kill("SIGKILL");
  } catch {}
  killWorkerGroup();
  try {
    model?.close();
  } catch {}
}
process.on("exit", cleanup);
process.on("SIGINT", () => process.exit(130));
process.on("SIGTERM", () => process.exit(143));
function fail(m, ev) {
  console.error(`[e2e-cloud ${el()}] FAIL:`, m);
  if (ev !== undefined) console.error(JSON.stringify(ev, null, 1));
  summary.failed = m;
  writeFileSync(join(OUT, "summary.json"), JSON.stringify(summary, null, 2));
  process.exit(1);
}
const assert = (c, m, ev) => {
  if (!c) fail(m, ev);
};
async function waitFor(what, fn, ms) {
  const until = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > until) fail(`timed out waiting for ${what}`);
    await sleep(250);
  }
}

// --- scripted model ------------------------------------------------------
// Markers in the request decide behavior. HOLD-* holds the first call open
// until released or the connection drops; APPROVE-NOTE asks for an elevated
// journal.note and answers once a tool result is present.
const modelCalls = []; // { marker, n, at, outcome }
const NOTE = "cloud e2e elevated note";
function sse(res, text) {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end(
    `data: ${JSON.stringify({ choices: [{ delta: { content: text } }] })}\n\n` +
      `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n` +
      "data: [DONE]\n\n",
  );
}
model = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    // The request carries earlier turns as context: only the newest user
    // message names this turn (after a tool round the last message is the
    // tool result, so search backwards).
    let lastUser = "";
    try {
      const messages = JSON.parse(body).messages ?? [];
      const m = [...messages].reverse().find((x) => x.role === "user");
      lastUser =
        typeof m?.content === "string"
          ? m.content
          : JSON.stringify(m?.content ?? "");
    } catch {}
    const marker = /\b(HOLD-[A-Z]+|APPROVE-NOTE|MSG-[A-Za-z0-9-]+)\b/.exec(
      lastUser,
    )?.[1];
    const n = modelCalls.filter((c) => c.marker === marker).length + 1;
    const call = { marker: marker ?? "?", n, at: el(), outcome: "answered" };
    modelCalls.push(call);
    if (marker?.startsWith("HOLD-") && n === 1) {
      call.outcome = "held";
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write(`data: {"choices":[{"delta":{"content":"thinking "}}]}\n\n`);
      held.set(marker, {
        res,
        release: () => {
          call.outcome = "held-then-answered";
          res.end(
            `data: ${JSON.stringify({ choices: [{ delta: { content: `reply to ${marker}` } }] })}\n\n` +
              `data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n` +
              "data: [DONE]\n\n",
          );
        },
      });
      res.on("close", () => {
        if (call.outcome === "held") call.outcome = "held-dropped";
      });
      return;
    }
    if (marker === "APPROVE-NOTE") {
      const parsed = JSON.parse(body);
      const hasResult = (parsed.messages ?? []).some((m) => m.role === "tool");
      if (!hasResult) {
        call.outcome = "tool-call";
        res.writeHead(200, { "content-type": "text/event-stream" });
        const args = JSON.stringify({
          route: "elevated",
          input: { text: NOTE },
        });
        res.end(
          `data: ${JSON.stringify({ choices: [{ delta: { tool_calls: [{ index: 0, id: "call_note", function: { name: "journal_note", arguments: args } }] } }] })}\n\n` +
            `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}\n\n` +
            "data: [DONE]\n\n",
        );
        return;
      }
    }
    sse(res, `reply to ${marker ?? "?"}`);
  });
});
await new Promise((r) => model.listen(BASE, "127.0.0.1", r));
log("scripted model on", MODEL);

// --- Go state service ----------------------------------------------------
const bin = join(OUT, "state-dev");
log("building state-dev…");
if (
  spawnSync("go", ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"], {
    cwd: API_DIR,
    stdio: "inherit",
  }).status !== 0
)
  fail("go build failed");
const stateLog = join(OUT, "state-dev.log");
let stateStarts = 0;
async function startState() {
  stateStarts++;
  const out = openSync(stateLog, "a");
  svc = spawn(bin, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: DB_URL,
      SUMI_DB_CREATE: "1",
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_STATE_LISTEN: `127.0.0.1:${BASE + 1}`,
      SUMI_MODEL_CONNECTION_KEY: CONN_KEY,
      SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
      SUMI_CORE_WAKE_URL: WORKER,
      SUMI_CORE_WAKE_TOKEN: WAKE,
    },
    stdio: ["ignore", out, out],
  });
  await waitFor(
    "state-dev health",
    async () => {
      try {
        return (await fetch(`${STATE}/health`)).ok;
      } catch {
        return false;
      }
    },
    30_000,
  );
  log(`state-dev up (start #${stateStarts})`);
}
async function stopState() {
  const p = svc;
  svc = null;
  p.kill("SIGKILL");
  await new Promise((r) => p.once("exit", r));
}
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
await startState();

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const HUMAN = uuidv7();
const SELECTED = uuidv7();
const UNSELECTED = uuidv7();
{
  const h = await sreq("POST", "/internal/dev/humans", ADMIN, {
    human_id: HUMAN,
  });
  assert(h.status === 201, `seed human ${h.status}`, h.text);
  const c = await sreq("POST", "/internal/dev/model-connections", ADMIN, {
    human_id: HUMAN,
    name: "cloud e2e scripted",
    preset: "openai-chat",
    base_url: MODEL,
    model: "e2e-scripted",
    api_key: "e2e-scripted-key",
  });
  assert(c.status === 201, `seed connection ${c.status}`, c.text);
  const s = await sreq("POST", "/internal/dev/model-selections", ADMIN, {
    human_id: HUMAN,
    kind: "api",
    connection_id: c.json.connection_id,
  });
  assert(s.status === 200, `select ${s.status}`, s.text);
  for (const [id, human, name] of [
    [SELECTED, HUMAN, "cloud e2e selected"],
    [UNSELECTED, null, "cloud e2e unselected"],
  ]) {
    const p = await sreq("POST", "/internal/core/personas", ADMIN, {
      persona_id: id,
      human_id: human,
      display_name: name,
    });
    assert(p.status === 201, `create persona ${p.status}`, p.text);
  }
}
const P = (id) => `/internal/core/personas/${id}`;

// --- workerd with the alpha environment's shape --------------------------
function jsonc(text) {
  // Strip // comments outside strings, then trailing commas.
  let out = "";
  let inStr = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inStr) {
      out += ch;
      if (ch === "\\") out += text[++i];
      else if (ch === '"') inStr = false;
    } else if (ch === '"') {
      inStr = true;
      out += ch;
    } else if (ch === "/" && text[i + 1] === "/") {
      while (i < text.length && text[i] !== "\n") i++;
      out += "\n";
    } else out += ch;
  }
  return JSON.parse(out.replace(/,(\s*[}\]])/g, "$1"));
}
const base = jsonc(readFileSync(join(CORE_DIR, "wrangler.jsonc"), "utf8"));
const alpha = base.env?.alpha;
assert(alpha, "wrangler.jsonc has no env.alpha");
assert(
  alpha.vpc_services?.length === 1 &&
    alpha.vpc_services[0].binding === "SUMI_STATE",
  "env.alpha must bind SUMI_STATE to a VPC Service",
  alpha.vpc_services,
);
assert(
  alpha.vars?.SUMI_MODEL_PROVIDER === "none" &&
    !alpha.vars?.SUMI_CORE_WAKE_OPEN,
  "env.alpha must use provider none and no dev wake flag",
  alpha.vars,
);
const STANDIN = "sumi-cloud-e2e-vpc-standin";
writeFileSync(
  join(OUT, "vpc-standin.mjs"),
  `// Stand-in for a Workers VPC Service: the configured target decides where
// a request goes; the URL host only travels as a header.
export default {
  async fetch(request, env) {
    const u = new URL(request.url);
    const target = new URL(u.pathname + u.search, env.TARGET);
    const forwarded = new Request(target, request);
    forwarded.headers.set("x-standin-original-host", u.host);
    return fetch(forwarded);
  },
};
`,
);
writeFileSync(
  join(OUT, "vpc-standin.json"),
  JSON.stringify(
    {
      name: STANDIN,
      main: join(OUT, "vpc-standin.mjs"),
      compatibility_date: base.compatibility_date,
      compatibility_flags: ["global_fetch_private_origin"],
      vars: { TARGET: STATE },
    },
    null,
    2,
  ),
);
const coreConfig = {
  name: "sumi-core-cloud-e2e",
  main: resolve(CORE_DIR, base.main),
  compatibility_date: base.compatibility_date,
  compatibility_flags: [
    ...(alpha.compatibility_flags ?? []),
    "global_fetch_private_origin",
  ],
  durable_objects: alpha.durable_objects,
  migrations: base.migrations,
  services: [{ binding: "SUMI_STATE", service: STANDIN }],
  vars: {
    ...alpha.vars,
    SUMI_STATE_URL: "http://sumi-state.invalid:8080",
  },
};
writeFileSync(
  join(OUT, "core-alpha-local.json"),
  JSON.stringify(coreConfig, null, 2),
);
summary.core_config = coreConfig;

const persistDir = join(OUT, "wrangler-state");
let workerStarts = 0;
async function startWorker() {
  workerStarts++;
  const out = openSync(join(OUT, `workerd-${workerStarts}.log`), "a");
  worker = spawn(
    "pnpm",
    [
      "exec",
      "wrangler",
      "dev",
      "--config",
      join(OUT, "core-alpha-local.json"),
      "--config",
      join(OUT, "vpc-standin.json"),
      "--port",
      String(BASE + 2),
      "--ip",
      "127.0.0.1",
      "--inspector-port",
      String(BASE + 3),
      "--persist-to",
      persistDir,
      "--var",
      `SUMI_CORE_RUNTIME_TOKEN:${RUNTIME}`,
      "--var",
      `SUMI_CORE_WAKE_TOKEN:${WAKE}`,
      "--show-interactive-dev-session=false",
    ],
    {
      cwd: CORE_DIR,
      env: { ...process.env, CI: "1" },
      stdio: ["ignore", out, out],
      detached: true,
    },
  );
  await waitFor(
    "workerd health",
    async () => {
      try {
        return (await fetch(`${WORKER}/health`)).ok;
      } catch {
        return false;
      }
    },
    90_000,
  );
  log(`workerd up (start #${workerStarts}, process group ${worker.pid})`);
}
async function stopWorker() {
  const pgid = worker.pid;
  killWorkerGroup("SIGKILL");
  worker = null;
  await waitFor(
    "workerd process group gone",
    async () => {
      const r = spawnSync("ps", ["-o", "pid=", "-g", String(pgid)], {
        encoding: "utf8",
      });
      return r.stdout.trim() === "";
    },
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
}

async function submit(persona, text) {
  const inputId = `in-${randomUUID()}`;
  const r = await sreq("POST", `${P(persona)}/inputs`, ADMIN, {
    input_id: inputId,
    kind: "message",
    payload: { text },
    actor_kind: "human",
    actor_id: HUMAN,
    source_surface: "cloud-e2e",
  });
  assert(r.status === 201, `submit ${r.status}`, r.text);
  return { inputId, at: Date.now() };
}
const outbox = async (persona) =>
  (await sreq("GET", `${P(persona)}/outbox?after_seq=0&limit=1000`, ADMIN)).json
    ?.outbox ?? [];
const forInput = (entries, inputId, kind) =>
  entries.filter((o) => o.kind === kind && o.payload?.input_id === inputId);
// A turn's final text is its reply, recorded on turn_completed
// (secretary_message is only the message.send tool's effect).
const replies = (entries, marker) =>
  entries.filter(
    (o) =>
      o.kind === "turn_completed" &&
      o.payload?.output?.text === `reply to ${marker}`,
  );
async function inputRow(persona, inputId) {
  return (await sreq("GET", `${P(persona)}/inputs/${inputId}`, ADMIN)).json;
}
async function completedOnce(persona, inputId, marker, ms) {
  await waitFor(
    `${marker}: turn completed`,
    async () =>
      forInput(await outbox(persona), inputId, "turn_completed").length > 0,
    ms,
  );
  await sleep(1_500); // a duplicate would land now
  const all = await outbox(persona);
  assert(
    forInput(all, inputId, "turn_completed").length === 1,
    `${marker}: turn_completed exactly once`,
    all,
  );
  assert(
    replies(all, marker).length === 1,
    `${marker}: exactly one reply`,
    all,
  );
}
const wakeLogSince = (offset) => readFileSync(stateLog, "utf8").slice(offset);

await startWorker();

// --- S0: routes ------------------------------------------------------------
{
  const w = (token, persona = SELECTED) =>
    fetch(`${WORKER}/personas/${persona}/wake`, {
      method: "POST",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  const noAuth = (await w()).status;
  const wrong = (await w(`${WAKE}x`)).status;
  const badPersona = (await w(WAKE, "not-a-persona")).status;
  const stateNoAuth = (await fetch(`${WORKER}/health/state`)).status;
  const stateRes = await fetch(`${WORKER}/health/state`, {
    headers: { Authorization: `Bearer ${WAKE}` },
  });
  const stateBody = await stateRes.json();
  log(
    `S0 routes: wake no-auth=${noAuth} wrong=${wrong} bad-persona=${badPersona}; /health/state no-auth=${stateNoAuth} auth=${stateRes.status} ${JSON.stringify(stateBody)}`,
  );
  assert(noAuth === 401 && wrong === 401, "wake must require the bearer");
  assert(badPersona === 400, "malformed persona must be refused");
  assert(stateNoAuth === 401, "/health/state must require the bearer");
  assert(
    stateRes.status === 200 && stateBody.via === "binding",
    "/health/state through the binding",
    stateBody,
  );
  summary.scenarios.routes = {
    wake_no_auth: noAuth,
    wake_wrong_token: wrong,
    wake_bad_persona: badPersona,
    state_health_no_auth: stateNoAuth,
    state_health: stateBody,
  };
}

// --- S1: ordinary conversation, woken by the Go sweep -----------------------
{
  const latencies = [];
  for (const marker of ["MSG-first", "MSG-second"]) {
    const { inputId, at } = await submit(SELECTED, `${marker} hello`);
    await completedOnce(SELECTED, inputId, marker, 30_000);
    latencies.push(Date.now() - at);
    // Let the fetch drain end and release the writer, so the next message
    // needs a fresh wake.
    await waitFor(
      "writer released",
      async () => {
        const st = await sreq("GET", `${P(SELECTED)}/state`, ADMIN);
        const exp = st.json?.lease?.expires_at;
        return !exp || Date.parse(exp) <= Date.now();
      },
      60_000,
    );
  }
  log(`S1 conversation: two replies, submit→completed ms ${latencies}`);
  summary.scenarios.conversation = { submit_to_completed_ms: latencies };
}

// --- S2: workerd killed mid-turn --------------------------------------------
{
  const marker = "HOLD-WORKER";
  const { inputId } = await submit(SELECTED, `${marker} please`);
  await waitFor("held model call", async () => held.has(marker), 30_000);
  const before = await inputRow(SELECTED, inputId);
  log(`S2 killing workerd mid-turn; input status ${before?.input?.status}`);
  await stopWorker();
  const killedAt = Date.now();
  const logOffset = readFileSync(stateLog, "utf8").length;
  await sleep(2_000);
  assert(
    replies(await outbox(SELECTED), marker).length === 0,
    "no reply while the runtime is gone",
  );
  await startWorker();
  await completedOnce(SELECTED, inputId, marker, 90_000);
  const calls = modelCalls.filter((c) => c.marker === marker);
  log(
    `S2 recovered after ${Date.now() - killedAt}ms; model calls ${JSON.stringify(calls)}`,
  );
  summary.scenarios.worker_killed_mid_turn = {
    input_status_before_kill: before?.input?.status,
    recovered_ms_after_kill: Date.now() - killedAt,
    model_calls: calls,
    state_log: wakeLogSince(logOffset).split("\n").filter(Boolean).slice(-6),
  };
}

// --- S3: workerd down when a message is admitted -----------------------------
{
  const marker = "MSG-while-down";
  await stopWorker();
  const logOffset = readFileSync(stateLog, "utf8").length;
  const { inputId, at } = await submit(SELECTED, `${marker} are you there`);
  await waitFor(
    "a failed wake logged",
    async () => /not woken \(will retry\)/.test(wakeLogSince(logOffset)),
    15_000,
  );
  const downFor = Date.now() - at;
  await startWorker();
  const startedAt = Date.now();
  await completedOnce(SELECTED, inputId, marker, 90_000);
  const lines = wakeLogSince(logOffset).split("\n").filter(Boolean);
  log(
    `S3 admitted while down; replied ${Date.now() - startedAt}ms after workerd returned`,
  );
  summary.scenarios.worker_down_at_admission = {
    down_ms: downFor,
    replied_ms_after_restart: Date.now() - startedAt,
    state_log: lines.slice(0, 3).concat(lines.slice(-3)),
  };
}

// --- S4: state service killed mid-turn ---------------------------------------
{
  const marker = "HOLD-STATE";
  const { inputId } = await submit(SELECTED, `${marker} please`);
  await waitFor("held model call", async () => held.has(marker), 30_000);
  await stopState();
  const probe = await fetch(`${WORKER}/health/state`, {
    headers: { Authorization: `Bearer ${WAKE}` },
  });
  const probeBody = await probe.json();
  log(
    `S4 state killed mid-turn; /health/state=${probe.status} ${JSON.stringify(probeBody)}`,
  );
  assert(
    probe.status === 503,
    "/health/state must report the outage",
    probeBody,
  );
  // The model answers while state is gone: the plan cannot be saved.
  held.get(marker).release();
  await sleep(3_000);
  const downAt = Date.now();
  await startState();
  await completedOnce(SELECTED, inputId, marker, 120_000);
  const calls = modelCalls.filter((c) => c.marker === marker);
  log(
    `S4 completed ${Date.now() - downAt}ms after state returned; model calls ${JSON.stringify(calls)}`,
  );
  summary.scenarios.state_killed_mid_turn = {
    state_health_during_outage: { status: probe.status, body: probeBody },
    completed_ms_after_state_restart: Date.now() - downAt,
    model_calls: calls,
  };
}

// --- S5: an unselected persona fails visibly --------------------------------
{
  const marker = "MSG-unselected";
  const callsBefore = modelCalls.filter((c) => c.marker === marker).length;
  const { inputId } = await submit(UNSELECTED, `${marker} hello`);
  const failed = await waitFor(
    "unselected: turn_failed",
    async () => forInput(await outbox(UNSELECTED), inputId, "turn_failed")[0],
    60_000,
  );
  await sleep(1_500);
  const all = await outbox(UNSELECTED);
  const modelCallsFor = modelCalls.filter((c) => c.marker === marker).length;
  log(`S5 unselected persona: turn_failed ${JSON.stringify(failed.payload)}`);
  assert(modelCallsFor === callsBefore, "no model was consulted");
  assert(
    all.filter(
      (o) => o.kind === "turn_completed" || o.kind === "secretary_message",
    ).length === 0,
    "no secretary reply for an unselected persona",
    all,
  );
  assert(
    JSON.stringify(failed.payload).includes("no model connection is selected"),
    "the failure names the missing selection",
    failed,
  );
  summary.scenarios.unselected_persona = { turn_failed: failed.payload };
}

// --- S6: approval parks and resumes after the human decision ----------------
{
  const marker = "APPROVE-NOTE";
  const { inputId } = await submit(SELECTED, `${marker} write it down`);
  const approval = await waitFor(
    "pending approval",
    async () =>
      (
        await sreq("GET", `${P(SELECTED)}/approvals`, ADMIN)
      ).json?.approvals?.find(
        (a) => a.input_id === inputId && a.status === "pending",
      ),
    30_000,
  );
  // Runtime credential cannot decide what the runtime parked.
  const byRuntime = await sreq(
    "POST",
    `${P(SELECTED)}/approvals/${approval.approval_id}/decision`,
    RUNTIME,
    {
      decision: "approve_once",
      decision_id: "runtime-attempt",
      decided_by_kind: "human",
      decided_by_id: HUMAN,
    },
  );
  assert(byRuntime.status === 401, "runtime credential decided an approval");
  await sleep(2_000); // the DO releases its writer while the input waits
  const decidedAt = Date.now();
  const d = await sreq(
    "POST",
    `${P(SELECTED)}/approvals/${approval.approval_id}/decision`,
    ADMIN,
    {
      decision: "approve_once",
      decision_id: "human-1",
      decided_by_kind: "human",
      decided_by_id: HUMAN,
    },
  );
  assert(d.status === 200, `decide ${d.status}`, d.text);
  await completedOnce(SELECTED, inputId, marker, 60_000);
  const events =
    (await sreq("GET", `${P(SELECTED)}/events?after_seq=0&limit=2000`, ADMIN))
      .json?.events ?? [];
  const notes = events.filter(
    (e) => e.kind === "note" && e.payload?.text === NOTE,
  );
  assert(notes.length === 1, `elevated note written ${notes.length} times`);
  log(
    `S6 approval: runtime decision refused (401); resumed ${Date.now() - decidedAt}ms after the human decision; note once`,
  );
  summary.scenarios.approval_resume = {
    runtime_decision_status: byRuntime.status,
    completed_ms_after_decision: Date.now() - decidedAt,
    notes: notes.length,
  };
}

summary.model_calls = modelCalls;
summary.database = dbUrl.pathname.slice(1);
writeFileSync(join(OUT, "summary.json"), JSON.stringify(summary, null, 2));
log(`PASS — summary in ${OUT}`);
process.exit(0);
