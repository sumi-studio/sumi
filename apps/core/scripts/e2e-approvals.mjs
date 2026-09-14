#!/usr/bin/env node
/**
 * E2E: tool approval on the REAL Go state service + REAL PostgreSQL + real
 * Node core processes. The model is the scripted MockProvider (directives
 * in the input text), not a real model.
 *
 * Covers: message.send waits for a human decision across process restarts
 * and sends exactly once after approval; a denial is never delivered; an
 * elevated journal.note parks, survives SIGKILL of the waiting host, and
 * runs exactly once after approval.
 *
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:55432/sumi?sslmode=disable \
 *     [SUMI_E2E_PORT=9540] node scripts/e2e-approvals.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import http from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error("e2e: SUMI_TEST_DB_URL (or SUMI_DB_URL) required — real PostgreSQL");
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const PORT = Number(process.env.SUMI_E2E_PORT ?? 8390 + (process.pid % 1000));
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const personaId = uuidv7();
const DECIDER = uuidv7();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(...a) {
  console.log("[e2e-approvals]", ...a);
}
function fail(msg) {
  console.error("[e2e-approvals] FAIL:", msg);
  process.exit(1);
}
function assert(cond, msg) {
  if (!cond) fail(msg);
}

async function req(method, path, token, body) {
  const res = await fetch(BASE + path, {
    method,
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
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

const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-approvals-"));
const bin = join(binDir, "state-dev");
log("building state-dev…");
if (
  spawnSync("go", ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"], {
    cwd: API_DIR,
    stdio: "inherit",
  }).status !== 0
) {
  fail("go build failed");
}
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
for (let up = false, deadline = Date.now() + 15_000; !up; ) {
  if (Date.now() > deadline) fail("state-dev did not become healthy");
  try {
    up = (await fetch(`${BASE}/health`)).ok;
  } catch {
    await sleep(200);
  }
}

// Decisions are identity-scoped: the persona must be bound to the deciding
// human, and the human row must exist. state-dev exposes a dev-only seeding
// route (humans are provisioned by the account flow on a real deployment).
const seeded = await req("POST", "/internal/dev/humans", ADMIN, { human_id: DECIDER });
assert(seeded.status === 201 || seeded.status === 200, `seed human ${seeded.status}: ${seeded.text}`);
const created = await req("POST", "/internal/core/personas", ADMIN, {
  persona_id: personaId,
  human_id: DECIDER,
  display_name: "e2e approvals",
});
assert(created.status === 201, `createPersona ${created.status}: ${created.text}`);
const ptoken = created.json.persona_token;
const P = `/internal/core/personas/${personaId}`;

const hostEnv = {
  ...process.env,
  SUMI_STATE_URL: BASE,
  SUMI_PERSONA_ID: personaId,
  SUMI_PERSONA_TOKEN: ptoken,
  SUMI_MODEL_PROVIDER: "mock",
  SUMI_LEASE_TTL_MS: "3000",
};
const HOST = join(CORE_DIR, "src/host/local.ts");

/** One fresh core process: acquire, recover, drain, exit. */
function once(label) {
  const r = spawnSync("node", [HOST, "--once"], {
    env: hostEnv,
    encoding: "utf8",
    timeout: 60_000,
  });
  log(`  core --once (${label}) exit=${r.status}`);
  if (r.status !== 0) {
    console.error(r.stdout, r.stderr);
    fail(`core --once (${label}) exited ${r.status}`);
  }
}
async function submit(text) {
  const inputId = `in-${randomUUID()}`;
  const r = await req("POST", `${P}/inputs`, ptoken, {
    input_id: inputId,
    kind: "message",
    payload: { text },
    actor_kind: "human",
    actor_id: DECIDER,
    source_surface: "e2e",
  });
  assert(r.status === 201, `submit ${r.status}: ${r.text}`);
  return inputId;
}
const outbox = async () => (await req("GET", `${P}/outbox?after_seq=0`, ptoken)).json.outbox;
const events = async () => (await req("GET", `${P}/events?after_seq=0&limit=1000`, ptoken)).json.events;
async function approvalFor(inputId) {
  const r = await req("GET", `${P}/approvals`, ptoken);
  return r.json.approvals.filter((a) => a.input_id === inputId);
}
function decide(approvalId, decision, decisionId, token = ADMIN) {
  return req("POST", `${P}/approvals/${approvalId}/decision`, token, {
    decision,
    decision_id: decisionId,
    decided_by_kind: "human",
    decided_by_id: DECIDER,
  });
}
const messagesWith = async (text) =>
  (await outbox()).filter((o) => o.kind === "secretary_message" && o.payload.text === text);

// --- scenario 1: approval after restarts sends exactly once -----------------
log("scenario 1: elevated message.send waits for approval across restarts, then sends once");
const SENT = "e2e: 会議を15時に移します";
const in1 = await submit(`!elevated message.send {"text":"${SENT}"}`);
once("park");
let [a1] = await approvalFor(in1);
assert(a1?.status === "pending" && a1.tool === "message.send" && a1.required_by === "route",
  `expected pending route approval, got ${JSON.stringify(a1)}`);
let st = (await req("GET", `${P}/state`, ptoken)).json;
assert(st.waiting_inputs === 1 && st.pending_approvals === 1 && st.running_turn === null,
  `state while waiting: ${JSON.stringify(st)}`);
assert((await outbox()).filter((o) => o.kind === "approval_requested").length === 1,
  "approval request not surfaced once on the outbox");
once("still waiting");
assert((await messagesWith(SENT)).length === 0, "message sent before approval");
assert((await decide(a1.approval_id, "approve_once", "d-1", ptoken)).status === 401,
  "persona token must not decide");
const approved = await decide(a1.approval_id, "approve_once", "d-1");
assert(approved.status === 200 && approved.json.approval.status === "approved",
  `approve: ${approved.status} ${approved.text}`);
once("resume");
once("after resume");
assert((await messagesWith(SENT)).length === 1, "approved message not sent exactly once");
const evs1 = await events();
assert(evs1.filter((e) => e.kind === "input_received" && e.payload.input_id === in1).length === 1,
  "resumed input journaled more than once");
assert((await outbox()).filter((o) => o.kind === "turn_completed" && o.payload.input_id === in1).length === 1,
  "resumed request did not complete once");
[a1] = await approvalFor(in1);
assert(a1.consumed_at, "approval grant not consumed");
assert((await decide(a1.approval_id, "deny_once", "d-2")).status === 409,
  "a later conflicting decision must be rejected");
log("  sent once after approval; journal shows the input once");

// --- scenario 2: denial is preserved -----------------------------------------
log("scenario 2: a denied elevated message.send is never delivered");
const DENIED = "e2e: 全員に一斉送信";
const in2 = await submit(`!elevated message.send {"text":"${DENIED}"}`);
once("park");
const [a2] = await approvalFor(in2);
assert(a2?.status === "pending", "denial scenario did not park");
assert((await decide(a2.approval_id, "deny_once", "d-1")).status === 200, "deny failed");
once("after deny");
once("again");
assert((await messagesWith(DENIED)).length === 0, "denied message was delivered");
const done2 = (await outbox()).find((o) => o.kind === "turn_completed" && o.payload.input_id === in2);
assert(done2?.payload.output.tool_results[0]?.result?.error === "denied",
  `model was not told of the denial: ${JSON.stringify(done2?.payload)}`);
assert((await decide(a2.approval_id, "approve_once", "d-2")).status === 409,
  "approval after denial must be rejected");
log("  denial preserved; reply reflects it");

// --- scenario 3: elevated route + SIGKILL of the waiting host -----------------
log("scenario 3: elevated journal.note parks, host is SIGKILLed, approval runs it once");
const NOTE = "e2e elevated note";
const in3 = await submit(`!elevated journal.note {"text":"${NOTE}"}`);
const runner = spawn("node", [HOST], { env: hostEnv, stdio: ["ignore", "pipe", "pipe"] });
runner.stdout.on("data", (d) => process.stdout.write(`[core-long] ${d}`));
let a3;
for (let i = 0; i < 75 && !a3; i++) {
  await sleep(200);
  [a3] = await approvalFor(in3);
}
assert(a3?.status === "pending" && a3.required_by === "route" && a3.route === "elevated",
  `elevated call did not park: ${JSON.stringify(a3)}`);
runner.kill("SIGKILL");
log("  waiting host SIGKILLed");
assert((await decide(a3.approval_id, "approve_once", "d-1")).status === 200, "approve elevated failed");
let took = false;
for (let i = 0; i < 20 && !took; i++) {
  await sleep(1000);
  const r = spawnSync("node", [HOST, "--once"], { env: hostEnv, encoding: "utf8", timeout: 60_000 });
  took = r.status === 0;
  const why = took ? "" : ` (${`${r.stderr}`.split("\n").find((l) => l.includes("fatal:")) ?? "no fatal line"})`;
  log(`  core --once after kill attempt ${i + 1} exit=${r.status}${why}`);
}
assert(took, "new host never acquired the lease after SIGKILL");
once("after recovery");
const notes = (await events()).filter((e) => e.kind === "note" && e.payload.text === NOTE);
assert(notes.length === 1, `elevated note written ${notes.length} times`);
assert((await outbox()).filter((o) => o.kind === "turn_completed" && o.payload.input_id === in3).length === 1,
  "elevated request did not complete once");

// --- scenario 4: a normal-route message.send never asks the human -----------
log("scenario 4: normal message.send is a structured block, not a prompt");
const BLOCKED = "e2e: normal send must not ask";
const in4 = await submit(`!message.send {"text":"${BLOCKED}"}`);
once("normal send");
assert((await approvalFor(in4)).length === 0, "a normal call created an approval");
assert((await messagesWith(BLOCKED)).length === 0, "blocked send was delivered");
const done4 = (await outbox()).find((o) => o.kind === "turn_completed" && o.payload.input_id === in4);
assert(done4, "blocked input did not complete");
assert(done4.payload.output.tool_results[0]?.result?.error === "blocked",
  `model was not told the call is blocked: ${JSON.stringify(done4?.payload)}`);
assert((await outbox()).filter((o) => o.kind === "approval_requested" && o.payload.input_id === in4).length === 0,
  "a normal call surfaced an approval request");
log("  structured block recorded; the human was never asked");

// --- scenario 5: a long human wait does not consume the provider retry window
// Real OpenAI-provider path against a loopback stub: park, wait far beyond
// the configured retry budget, approve, then a transient 500 on the resumed
// consultation must still retry — the send lands exactly once.
log("scenario 5: long approval wait + transient provider error still recovers");
const LONGWAIT = "e2e: sent after a long human wait";
const STUB_PORT = PORT + 7;
let stubCalls = 0;
const stub = http.createServer((rq, rs) => {
  let body = "";
  rq.on("data", (c) => (body += c));
  rq.on("end", () => {
    stubCalls++;
    // Connection: close — a pooled keep-alive socket would keep the host's
    // event loop (and its `main()` return) alive past `secretary.stop()`.
    const sse = (lines) => {
      rs.writeHead(200, {
        "Content-Type": "text/event-stream",
        Connection: "close",
      });
      rs.end(lines.map((l) => `data: ${l}`).join("\n") + "\ndata: [DONE]\n\n");
    };
    if (stubCalls === 1) {
      // Round 0: an elevated message.send the human must approve.
      sse([
        `{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_send","function":{"name":"message_send","arguments":${JSON.stringify(JSON.stringify({ route: "elevated", input: { text: LONGWAIT } }))}}}]}}]}`,
        `{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
      ]);
    } else if (stubCalls === 2) {
      // Round 1 after approval: one transient provider failure.
      rs.writeHead(500, { Connection: "close" }).end("stub transient");
    } else {
      sse([
        `{"choices":[{"delta":{"content":"sent after the wait"}}]}`,
        `{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"total_tokens":3}}`,
      ]);
    }
  });
});
await new Promise((r) => stub.listen(STUB_PORT, "127.0.0.1", r));
const hostEnv5 = {
  ...process.env,
  SUMI_STATE_URL: BASE,
  SUMI_PERSONA_ID: personaId,
  SUMI_PERSONA_TOKEN: ptoken,
  SUMI_MODEL_PROVIDER: "openai",
  SUMI_MODEL_BASE_URL: `http://127.0.0.1:${STUB_PORT}`,
  SUMI_MODEL_API_KEY: "e2e-stub-key",
  SUMI_MODEL_MODEL: "stub-model",
  // 10s of active processing budget; the human waits ~11s.
  SUMI_PROVIDER_RETRY_BUDGET_MS: "10000",
  SUMI_LEASE_TTL_MS: "3000",
};
// spawn, not spawnSync: the stub server lives in this process's event loop —
// a synchronous spawn would starve it and the host's fetch would hang.
const once5 = (label) =>
  new Promise((resolve, reject) => {
    const p = spawn("node", [HOST, "--once"], { env: hostEnv5 });
    let out = "";
    p.stdout.on("data", (d) => (out += d));
    p.stderr.on("data", (d) => (out += d));
    const killer = setTimeout(() => p.kill("SIGKILL"), 90_000);
    p.on("exit", (code) => {
      clearTimeout(killer);
      log(`  core --once (${label}) exit=${code}`);
      if (code !== 0) {
        console.error(out);
        reject(new Error(`core --once (${label}) exited ${code}`));
      } else resolve();
    });
  });
const in5 = await submit("e2e: please send the long-wait message");
await once5("park");
const [a5] = await approvalFor(in5);
assert(a5?.status === "pending" && a5.required_by === "route",
  `long-wait call did not park: ${JSON.stringify(a5)}`);
let in5row = (await req("GET", `${P}/inputs/${in5}`, ptoken)).json.input;
assert(in5row.status === "waiting" && in5row.waiting_since, `input not waiting: ${JSON.stringify(in5row)}`);
log("  parked; waiting ~11s past the 10s retry budget before approving");
await sleep(11_000);
assert((await decide(a5.approval_id, "approve_once", "d-1")).status === 200, "approve failed");
in5row = (await req("GET", `${P}/inputs/${in5}`, ptoken)).json.input;
assert(in5row.waited_ms >= 9_000, `waited_ms = ${in5row.waited_ms} — human wait not recorded`);
await once5("resume + transient retry");
await once5("final drain");
assert((await messagesWith(LONGWAIT)).length === 1, "granted send did not run exactly once");
assert((await outbox()).filter((o) => o.kind === "turn_failed" && o.payload.input_id === in5).length === 0,
  "a transient provider error after the long wait was terminal");
assert((await outbox()).filter((o) => o.kind === "turn_completed" && o.payload.input_id === in5).length === 1,
  "long-wait request did not complete");
assert(stubCalls >= 3, `stub saw ${stubCalls} calls — the transient error was never retried`);
log("  wait excluded from the budget; send landed once, request completed");
stub.close();

svc.kill("SIGKILL");
log("PASS — approval scenarios green on real PG + real Go + real Node (scripted mock + stub models)");
