#!/usr/bin/env node
/**
 * E2E: the authenticated browser approval surface over the REAL production
 * server (cmd/server), REAL PostgreSQL, and a REAL Node TypeScript core.
 *
 * Covers the human-facing path end to end:
 *   session cookie → GET /me/approvals → pending durable row
 *   → POST /me/approvals/{id}/decision → same suspended input resumes and
 *   the granted effect runs exactly once; a denial runs nothing and the
 *   model receives the refusal. Another human's session cannot see or act
 *   on the parked grant, identical replays are idempotent, and a later
 *   conflicting decision is refused.
 *
 * Fixtures (clearly identified, no real credentials):
 *   - cmd/e2e-session-cookie mints the Human row + secretary binding and
 *     issues a real signed browser session cookie (HMAC secret from env).
 *   - The model is the scripted MockProvider (directives in input text).
 *   - SUMI_E2E_PG_CONTAINER names a docker Postgres the script creates an
 *     empty database in (and drops on exit); otherwise SUMI_TEST_DB_URL must
 *     point at an empty database used directly.
 *
 *   SUMI_E2E_PG_CONTAINER=sumi-approval-outbox-20260915-pg \
 *     [SUMI_E2E_PORT=11471] node scripts/e2e-browser-approvals.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const CORE_DIR = resolve(import.meta.dirname, "..");
const API_DIR = resolve(CORE_DIR, "../api");
const PORT = Number(process.env.SUMI_E2E_PORT ?? 11471);
const BASE = `http://127.0.0.1:${PORT}`;
const PG_CONTAINER = process.env.SUMI_E2E_PG_CONTAINER;
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}

const HUMAN_A = uuidv7();
const HUMAN_B = uuidv7();
const SECRETARY = uuidv7();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(...a) {
  console.log("[e2e-browser-approvals]", ...a);
}
function fail(msg) {
  console.error("[e2e-browser-approvals] FAIL:", msg);
  process.exit(1);
}
function assert(cond, msg) {
  if (!cond) fail(msg);
}

// --- disposable database ----------------------------------------------------
let dbURL = process.env.SUMI_TEST_DB_URL;
let createdDB = "";
if (PG_CONTAINER) {
  createdDB = `sumi_appr_e2e_${randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const r = spawnSync("docker", [
    "exec", PG_CONTAINER, "psql", "-U", "sumi", "-d", "postgres",
    "-c", `CREATE DATABASE "${createdDB}"`,
  ], { encoding: "utf8" });
  if (r.status !== 0) fail(`create database: ${r.stderr || r.stdout}`);
  // Reach the container through its published port on the host loopback.
  const inspect = spawnSync("docker", [
    "inspect", PG_CONTAINER,
    "--format", "{{range $p, $b := .NetworkSettings.Ports}}{{$p}}->{{(index $b 0).HostPort}}{{end}}",
  ], { encoding: "utf8" });
  const published = /5432\/tcp->(\d+)/.exec(inspect.stdout ?? "");
  if (!published) fail(`no published 5432 port on ${PG_CONTAINER}`);
  dbURL = `postgres://sumi:sumi-dev@127.0.0.1:${published[1]}/${createdDB}?sslmode=disable`;
}
if (!dbURL) {
  console.error("e2e: SUMI_E2E_PG_CONTAINER or SUMI_TEST_DB_URL required");
  process.exit(2);
}
process.on("exit", () => {
  if (createdDB) {
    spawnSync("docker", [
      "exec", PG_CONTAINER, "psql", "-U", "sumi", "-d", "postgres",
      "-c", `DROP DATABASE IF EXISTS "${createdDB}" WITH (FORCE)`,
    ]);
  }
});

// --- build + start the production server -------------------------------------
const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-browser-approvals-"));
const serverBin = join(binDir, "server");
const issuerBin = join(binDir, "e2e-session-cookie");
log("building cmd/server + cmd/e2e-session-cookie…");
for (const [out, pkg] of [[serverBin, "./cmd/server"], [issuerBin, "./cmd/e2e-session-cookie"]]) {
  if (spawnSync("go", ["build", "-buildvcs=false", "-o", out, pkg], {
    cwd: API_DIR, stdio: "inherit",
  }).status !== 0) fail("go build failed");
}
const runtimeDir = mkdtempSync(join(tmpdir(), "sumi-e2e-runtime-"));
const sessionSecret = randomBytes(48).toString("base64");
const sessionAudience = "sumi:web";
const svc = spawn(serverBin, [], {
  env: {
    ...process.env,
    PORT: String(PORT),
    SUMI_PUBLIC_LOOPBACK_LISTEN: `127.0.0.1:${PORT}`,
    SUMI_DB_URL: dbURL,
    SUMI_AGENT_RUNTIME_STATE_DIR: join(runtimeDir, "gateway"),
    SUMI_COMMAND_LOG_DIR: join(runtimeDir, "commands"),
    SUMI_BROWSER_SESSION_SECRET: sessionSecret,
    SUMI_BROWSER_SESSION_AUDIENCE: sessionAudience,
    SUMI_BROWSER_WS_ALLOWED_ORIGINS: BASE,
    SUMI_AUTH_ALLOW_INSECURE_COOKIES: "true",
    // Auth routes mount (the SPA checks /auth/session); the Firebase admin
    // verifier is only contacted on a real login exchange, which this
    // fixture never performs — the emulator address is deliberately dead.
    SUMI_AUTH_FIREBASE_PROJECT_ID: "sumi-studio",
    SUMI_AUTH_TENANT_ID: "e2e-approvals",
    FIREBASE_AUTH_EMULATOR_HOST: "127.0.0.1:9",
    SUMI_AGENT_WRAPPING_KEY_ID: `e2e-${randomUUID().slice(0, 8)}`,
    SUMI_CORE_STATE_TOKEN: ADMIN,
  },
  stdio: ["ignore", "pipe", "pipe"],
});
svc.stderr.on("data", (d) => process.stderr.write(`[server] ${d}`));
process.on("exit", () => svc.kill("SIGKILL"));
for (let up = false, deadline = Date.now() + 30_000; !up; ) {
  if (Date.now() > deadline) fail("server did not become healthy");
  try {
    up = (await fetch(`${BASE}/health`)).ok;
  } catch {
    await sleep(250);
  }
}
log("server healthy on", BASE);

// --- sessions: two humans; A owns the secretary ------------------------------
function issueSession(userID, personalityAgentID, provisionSecretary) {
  const r = spawnSync(issuerBin, [], {
    env: {
      ...process.env,
      SUMI_BROWSER_SESSION_SECRET: sessionSecret,
      SUMI_BROWSER_SESSION_AUDIENCE: sessionAudience,
      SUMI_E2E_SESSION_TENANT_ID: "e2e-approvals",
      SUMI_E2E_SESSION_USER_ID: userID,
      SUMI_E2E_SESSION_PERSONALITY_AGENT_ID: personalityAgentID,
      ...(provisionSecretary ? { SUMI_E2E_SESSION_PROVISION_SECRETARY: "1" } : {}),
      SUMI_E2E_SESSION_DATABASE_URL: dbURL,
      SUMI_E2E_SESSION_DISPLAY_NAME: "E2E Human",
    },
    encoding: "utf8",
  });
  if (r.status !== 0) fail(`session issuer: ${r.stderr}`);
  const cookie = r.stdout.trim();
  assert(cookie.length > 0 && cookie.length < 4096 && !/\s/.test(cookie),
    "invalid session cookie");
  return cookie;
}
const cookieA = issueSession(HUMAN_A, SECRETARY, true);
const cookieB = issueSession(HUMAN_B, uuidv7(), false);

// --- helpers ------------------------------------------------------------------
async function req(method, path, { token, cookie, origin } = {}, body) {
  const headers = { "Content-Type": "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;
  if (cookie) headers.Cookie = `sumi_session=${cookie}`;
  if (origin) headers.Origin = origin;
  const res = await fetch(BASE + path, {
    method, headers,
    body: body === undefined ? undefined
      : typeof body === "string" ? body : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* non-JSON */ }
  return { status: res.status, json, text,
    cacheControl: res.headers.get("cache-control") };
}

// Bind the core persona to human A under the secretary's identity — the same
// binding CoreAttentionDelivery establishes in production.
const created = await req("POST", "/internal/core/personas", { token: ADMIN }, {
  persona_id: SECRETARY, human_id: HUMAN_A, display_name: "E2E Secretary",
});
assert(created.status === 201, `createPersona ${created.status}: ${created.text}`);
const ptoken = created.json.persona_token;
const P = `/internal/core/personas/${SECRETARY}`;

const hostEnv = {
  ...process.env,
  SUMI_STATE_URL: BASE,
  SUMI_PERSONA_ID: SECRETARY,
  SUMI_PERSONA_TOKEN: ptoken,
  SUMI_MODEL_PROVIDER: "mock",
  SUMI_LEASE_TTL_MS: "3000",
};
const HOST = join(CORE_DIR, "src/host/local.ts");
function once(label) {
  const r = spawnSync("node", [HOST, "--once"], {
    env: hostEnv, encoding: "utf8", timeout: 60_000,
  });
  log(`  core --once (${label}) exit=${r.status}`);
  if (r.status !== 0) {
    console.error(r.stdout, r.stderr);
    fail(`core --once (${label}) exited ${r.status}`);
  }
}
async function submit(text) {
  const inputId = `in-${randomUUID()}`;
  const r = await req("POST", `${P}/inputs`, { token: ptoken }, {
    input_id: inputId, kind: "message",
    payload: { text }, actor_kind: "human", actor_id: HUMAN_A,
    source_surface: "e2e",
  });
  assert(r.status === 201, `submit ${r.status}: ${r.text}`);
  return inputId;
}
const outbox = async () =>
  (await req("GET", `${P}/outbox?after_seq=0`, { token: ptoken })).json.outbox;
const inbox = async (cookie) =>
  req("GET", "/me/approvals", { cookie });
const decide = (cookie, approvalId, decision, decisionId, extra = {}) =>
  req("POST", `/me/approvals/${approvalId}/decision`,
    { cookie, origin: BASE }, { decision, decision_id: decisionId, ...extra });
const messagesWith = async (text) =>
  (await outbox()).filter((o) => o.kind === "secretary_message" && o.payload.text === text);

// --- scenario A: browser inbox sees the parked call, approval resumes it ------
log("scenario A: authenticated human sees and approves the parked send once");
const SENT = "e2e browser approval: 会議を15時に";
const inA = await submit(`!elevated message.send {"text":"${SENT}"}`);
once("park");

let inboxA = await inbox(cookieA);
assert(inboxA.status === 200, `inbox ${inboxA.status}: ${inboxA.text}`);
assert(inboxA.cacheControl === "no-store",
  `private inbox must not be cacheable (got ${inboxA.cacheControl})`);
assert(inboxA.json.human === HUMAN_A,
  `inbox must echo its session human (got ${inboxA.json.human})`);
let rowsA = inboxA.json.approvals.filter((a) => a.input_id === inA);
assert(rowsA.length === 1 && rowsA[0].status === "pending" &&
  rowsA[0].tool === "message.send" && rowsA[0].secretary_name === "E2E Secretary" &&
  rowsA[0].request.text === SENT,
  `inbox row: ${JSON.stringify(rowsA[0])}`);
const approvalA = rowsA[0].approval_id;

// Auth boundaries on the browser surface.
assert((await inbox("")).status === 401, "inbox without session must be 401");
const inboxB = await inbox(cookieB);
assert(inboxB.status === 200 && inboxB.json.approvals.length === 0,
  "another human's inbox must not list the grant");
assert((await decide(cookieB, approvalA, "deny_once", "d-b")).status === 403,
  "another human must not decide a copied approval id");
assert((await decide(cookieA, approvalA, "approve_once", "d-x",
  { decided_by_id: HUMAN_B })).status === 400,
  "a browser-supplied decider field must be rejected");
assert((await req("POST", `/me/approvals/${approvalA}/decision`,
  { cookie: cookieA, origin: "https://evil.example" },
  { decision: "approve_once", decision_id: "d-x" })).status === 403,
  "cross-origin decision must be refused");

// A decision body is exactly one JSON value — trailing data is a 400 and
// records nothing (the approval stays pending).
assert((await req("POST", `/me/approvals/${approvalA}/decision`,
  { cookie: cookieA, origin: BASE },
  `{"decision":"approve_once","decision_id":"d-tg"} {"extra":1}`)).status === 400,
  "a decision body with trailing JSON must be rejected");
inboxA = await inbox(cookieA);
assert(inboxA.json.approvals.find((a) => a.approval_id === approvalA)?.status === "pending",
  "rejected trailing-JSON body must not record a decision");

// The bound human approves; the decision is one-shot and idempotent.
const approved = await decide(cookieA, approvalA, "approve_once", "d-1");
assert(approved.cacheControl === "no-store",
  `decision response must not be cacheable (got ${approved.cacheControl})`);
assert(approved.status === 200 && approved.json.approval.status === "approved" &&
  approved.json.approval.decided_by_id === HUMAN_A,
  `approve: ${approved.status} ${approved.text}`);
assert(approved.json.approval.secretary_name === "E2E Secretary" &&
  approved.json.approval.input?.input_id === inA,
  `decision response must keep the enriched projection: ${approved.text}`);
assert((await decide(cookieA, approvalA, "approve_once", "d-1")).status === 200,
  "identical replay must succeed");
assert((await decide(cookieA, approvalA, "deny_once", "d-2")).status === 409,
  "conflicting decision must be rejected");

once("resume");
once("after resume");
assert((await messagesWith(SENT)).length === 1,
  "approved message did not land exactly once");
inboxA = await inbox(cookieA);
rowsA = inboxA.json.approvals.filter((a) => a.approval_id === approvalA);
assert(rowsA.length === 1 && rowsA[0].status === "approved" && rowsA[0].consumed_at,
  `resolved inbox row: ${JSON.stringify(rowsA[0])}`);
log("  approved send ran once; inbox now shows the resolved row");

// --- scenario B: a denial runs nothing and the model hears the refusal -------
log("scenario B: deny performs no side effect and refuses the same request");
const DENIED = "e2e browser denial: 全員送信";
const inB = await submit(`!elevated message.send {"text":"${DENIED}"}`);
once("park");
inboxA = await inbox(cookieA);
const [rowB] = inboxA.json.approvals.filter((a) => a.input_id === inB);
assert(rowB?.status === "pending", "denial scenario did not park");
const denied = await decide(cookieA, rowB.approval_id, "deny_once", "d-1");
assert(denied.status === 200 && denied.json.approval.status === "denied",
  `deny: ${denied.status} ${denied.text}`);
once("after deny");
once("again");
assert((await messagesWith(DENIED)).length === 0, "denied message was delivered");
const doneB = (await outbox()).find(
  (o) => o.kind === "turn_completed" && o.payload.input_id === inB);
assert(doneB?.payload.output.tool_results[0]?.result?.error === "denied",
  `model was not told of the denial: ${JSON.stringify(doneB?.payload)}`);
assert((await decide(cookieA, rowB.approval_id, "approve_once", "d-2")).status === 409,
  "approval after denial must be rejected");
log("  denial preserved; resumed request carries the refusal");

// --- scenario C: a persona sealed for transfer is not actionable here ------
log("scenario C: sealed persona's pending grant leaves the actionable inbox");
const SEALED = "e2e sealed approval: 通知して";
const inC = await submit(`!elevated message.send {"text":"${SEALED}"}`);
once("park for seal");
inboxA = await inbox(cookieA);
const [rowC] = inboxA.json.approvals.filter((a) => a.input_id === inC);
assert(rowC?.status === "pending", "seal scenario did not park");

// The real transfer step: seal this persona toward a destination placement.
const transferID = uuidv7();
const destination = uuidv7();
const sealed = await req("POST",
  `${P}/transfers/${transferID}/seal`, { token: ADMIN },
  { destination_id: destination });
assert(sealed.status === 200, `seal ${sealed.status}: ${sealed.text}`);

// The durable grant survives pending (it travels with the persona), but this
// placement no longer lists it as actionable and refuses the decision.
inboxA = await inbox(cookieA);
assert(!inboxA.json.approvals.some(
  (a) => a.approval_id === rowC.approval_id && a.status === "pending"),
  "sealed persona's grant must leave the actionable inbox");
const staleDecision = await decide(cookieA, rowC.approval_id, "approve_once", "d-seal");
assert(staleDecision.status === 409 && staleDecision.json.error === "persona_inactive",
  `sealed decision must be a terminal 409 persona_inactive: ${staleDecision.status} ${staleDecision.text}`);
const stored = await req("GET",
  `${P}/approvals/${rowC.approval_id}`, { token: ptoken });
assert(stored.status === 200 && stored.json.approval?.status === "pending" &&
  stored.json.approval?.decision === null,
  `sealed approval must stay pending for the destination: ${stored.status} ${stored.text}`);
assert((await messagesWith(SEALED)).length === 0,
  "refused sealed approval must produce no side effect");
log("  sealed grant is preserved for the destination and refused here");

svc.kill("SIGKILL");
log("PASS — browser approval surface verified on real server + PG + Node core (fixtures: e2e session issuer, mock provider)");
