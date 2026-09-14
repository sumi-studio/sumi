#!/usr/bin/env node
/**
 * E2E for usage accounting and budgets: REAL Go state service + REAL
 * PostgreSQL + a real Node core child streaming from a scripted
 * OpenAI-compatible provider over HTTP.
 *
 * Covers:
 *   1. Every provider call records one usage fact carrying the funding
 *      principal selected at call time (operator env here — the persona
 *      has no human/selection), with distinct token categories, the raw
 *      provider report, and a cost priced by the configured rate card.
 *   2. Re-delivering the same fact replays the stored row; a conflicting
 *      payload under the same fact_id is a 409; a genuinely additional
 *      call is a second fact.
 *   3. A configured budget denies admission BEFORE any request reaches
 *      the provider — the turn parks (awaiting, input waiting, outbox
 *      budget_wait) and spends no attempt; raising the cap resumes the
 *      parked input without duplicating work.
 *   4. A call whose provider never reported usage is recorded 'unknown'
 *      and inspectable — never silently zero.
 *   5. Memory preparation calls are metered too (phase 'memory').
 *
 * The provider and rates are labelled fixtures — not provider prices.
 *
 *   SUMI_TEST_DB_URL=postgres://… [SUMI_E2E_DIR=<artifacts>] \
 *   [SUMI_E2E_PORT=9541] node scripts/e2e-usage.mjs
 */
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const CHILD = process.argv.includes("--child");
if (CHILD) {
  await childMain();
} else {
  await main();
}

// ---------------------------------------------------------------- child ---
async function childMain() {
  const { Secretary } = await import("../src/secretary.ts");
  const { HttpStateClient } = await import("../src/state-client.ts");
  const { providerForPersona } = await import("../src/host/provider-env.ts");
  const env = (n) => {
    const v = process.env[n];
    if (!v) throw new Error(`missing env ${n}`);
    return v;
  };
  const state = new HttpStateClient(
    env("SUMI_STATE_URL"),
    env("SUMI_PERSONA_TOKEN"),
  );
  const secretary = new Secretary({
    personaId: env("SUMI_PERSONA_ID"),
    holderId: `usage-e2e-${process.pid}`,
    state,
    provider: providerForPersona(
      state,
      env("SUMI_PERSONA_ID"),
      (n) => process.env[n],
    ),
    leaseTtlMs: 3_000,
    renewEveryMs: 1_000,
    contextLimit: 5_000,
    pollIntervalMs: 100,
    scheduleEveryMs: 1_000,
    idgen: () => crypto.randomUUID(),
    log: (msg, fields) =>
      console.log(
        `[child ${process.pid}] ${msg}`,
        fields ? JSON.stringify(fields) : "",
      ),
  });
  const ac = new AbortController();
  process.on("SIGTERM", () => ac.abort());
  await secretary.run(ac.signal);
}

// --------------------------------------------------------------- parent ---
async function main() {
  const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
  if (!DB_URL) {
    console.error("e2e-usage: SUMI_TEST_DB_URL required — real PostgreSQL");
    process.exit(2);
  }
  const DIR =
    process.env.SUMI_E2E_DIR ??
    mkdtempSync(join(tmpdir(), "sumi-e2e-usage-run-"));
  mkdirSync(DIR, { recursive: true });
  process.env.SUMI_E2E_DIR = DIR;
  const API_DIR = resolve(import.meta.dirname, "../../api");
  const SELF = resolve(import.meta.dirname, "e2e-usage.mjs");
  const PORT = Number(process.env.SUMI_E2E_PORT ?? 9541);
  const BASE = `http://127.0.0.1:${PORT}`;
  const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;
  const children = [];

  const log = (...a) => console.log("[e2e-usage]", ...a);
  const fail = (msg, evidence) => {
    console.error("[e2e-usage] FAIL:", msg);
    if (evidence !== undefined)
      console.error(JSON.stringify(evidence, null, 1));
    for (const c of children) c.kill("SIGKILL");
    process.exit(1);
  };
  const check = (cond, msg, evidence) => {
    if (!cond) fail(msg, evidence);
  };
  process.on("uncaughtException", (e) => {
    console.error("[e2e-usage] FAIL:", e);
    for (const c of children) c.kill("SIGKILL");
    process.exit(1);
  });
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  function uuidv7() {
    const now = Date.now().toString(16).padStart(12, "0");
    const r = randomUUID().replaceAll("-", "");
    return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
  }
  const personaId = uuidv7();

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

  // --- scripted provider: one request → SSE text + usage report -----------
  // Fixture usage, not a real bill: 1000 prompt (400 cached) + 200 output.
  // A `drop-usage` file in DIR makes the stream omit the usage chunk —
  // the fact then records 'unknown'.
  const providerLog = join(DIR, "provider-requests.jsonl");
  const stub = createServer((reqIn, res) => {
    if (reqIn.method !== "POST" || !reqIn.url.endsWith("/chat/completions")) {
      res.writeHead(404).end();
      return;
    }
    let body = "";
    reqIn.on("data", (d) => (body += d));
    reqIn.on("end", () => {
      const parsed = JSON.parse(body);
      const last = parsed.messages.at(-1)?.content ?? "";
      appendFileSync(
        providerLog,
        `${JSON.stringify({
          model: parsed.model,
          chars: last.length,
          last: last.slice(0, 60),
        })}\n`,
      );
      const isMemory = last.includes("compact_target");
      const text = isMemory ? "KEEP_UNCHANGED" : `ack ${last.slice(0, 40)}`;
      const usage = existsSync(join(DIR, "drop-usage"))
        ? null
        : {
            prompt_tokens: 1000,
            completion_tokens: 200,
            prompt_tokens_details: { cached_tokens: 400 },
          };
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(
        [
          JSON.stringify({ choices: [{ delta: { content: text } }] }),
          JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] }),
          ...(usage ? [JSON.stringify({ usage })] : []),
          "[DONE]",
        ]
          .map((l) => `data: ${l}\n\n`)
          .join(""),
      );
    });
  });
  await new Promise((r) => stub.listen(0, "127.0.0.1", r));
  const stubPort = stub.address().port;
  const providerRequests = () =>
    existsSync(providerLog)
      ? readFileSync(providerLog, "utf8").split("\n").filter(Boolean).length
      : 0;

  // --- state service -------------------------------------------------------
  const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-usage-"));
  const bin = join(binDir, "state-dev");
  log("building state-dev…");
  const build = spawnSync(
    "go",
    ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"],
    { cwd: API_DIR, stdio: "inherit" },
  );
  if (build.status !== 0) fail("go build failed");

  log("starting state-dev on", BASE);
  const svcOut = openSync(join(DIR, "state-dev.log"), "a");
  const svc = spawn(bin, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: DB_URL,
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
    },
    stdio: ["ignore", svcOut, svcOut],
  });
  children.push(svc);
  process.on("exit", () => svc.kill("SIGKILL"));
  for (const deadline = Date.now() + 30_000; ; ) {
    try {
      if ((await fetch(`${BASE}/health`)).ok) break;
    } catch {
      /* not up */
    }
    if (Date.now() > deadline) fail("state-dev did not become healthy");
    await sleep(200);
  }

  const created = await req("POST", "/internal/core/personas", ADMIN, {
    persona_id: personaId,
    display_name: "e2e usage secretary",
  });
  check(
    created.status === 201,
    `createPersona ${created.status}`,
    created.text,
  );
  const ptoken = created.json.persona_token;
  const P = `/internal/core/personas/${personaId}`;
  const facts = async () =>
    (await req("GET", `${P}/usage/facts?limit=100`, ptoken)).json.facts;
  const outboxFor = async (inputId) =>
    (await req("GET", `${P}/outbox?after_seq=0`, ptoken)).json.outbox.filter(
      (o) => o.payload.input_id === inputId,
    );
  const inputStatus = async (inputId) =>
    (await req("GET", `${P}/inputs/${inputId}`, ptoken)).json.input?.status;

  // Fixture rate card, labelled — not provider prices.
  // 1 unit per input token, 2 per output token; no cached rate configured,
  // so cached input bills at the input rate:
  // (1000-400)*1 + 400*1 + 200*2 = 1400 units per reported call.
  const setBudget = (limit) =>
    req("PUT", "/internal/dev/usage-budgets", ADMIN, {
      funding_kind: "operator",
      funding_id: "env",
      limit_minor: limit,
      currency: "USD",
      rate_input_per_mtok: 1_000_000,
      rate_output_per_mtok: 2_000_000,
      pricing_revision: "fixture-rates-v1",
    });

  async function waitFor(fn, what, timeoutMs = 30_000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (await fn()) return;
      if (Date.now() > deadline) {
        fail(`timed out waiting for ${what}`, { facts: await facts() });
      }
      await sleep(100);
    }
  }
  async function say(text) {
    const inputId = `in-${randomUUID()}`;
    const r = await req("POST", `${P}/inputs`, ptoken, {
      input_id: inputId,
      kind: "message",
      payload: { text },
      actor_kind: "human",
      actor_id: "e2e",
      source_surface: "e2e",
    });
    check(r.status >= 200 && r.status < 300, `submit ${r.status}`, r.text);
    await waitFor(
      async () =>
        (await outboxFor(inputId)).some((o) => o.kind === "turn_completed"),
      `reply to "${text.slice(0, 32)}"`,
    );
    log("replied:", text.slice(0, 48));
    return inputId;
  }

  let b = await setBudget(1_000_000);
  check(b.status === 200, `seed budget ${b.status}`, b.text);

  const out = openSync(join(DIR, "child.log"), "a");
  const child = spawn(process.execPath, [SELF, "--child"], {
    env: {
      ...process.env,
      SUMI_STATE_URL: BASE,
      SUMI_PERSONA_ID: personaId,
      SUMI_PERSONA_TOKEN: ptoken,
      SUMI_MODEL_PROVIDER: "openai",
      SUMI_MODEL_BASE_URL: `http://127.0.0.1:${stubPort}`,
      SUMI_MODEL_API_KEY: "fixture-key",
      SUMI_MODEL_MODEL: "fixture-model",
    },
    stdio: ["ignore", out, out],
  });
  children.push(child);
  log(`child pid=${child.pid}`);

  // --- 1: two real calls → two reported facts on operator/env --------------
  await say("usage alpha one");
  await say("usage alpha two");
  await waitFor(async () => (await facts()).length >= 2, "two usage facts");
  let fs = await facts();
  const [f1, f2] = fs;
  for (const f of [f1, f2]) {
    check(f.kind === "model_call" && f.phase === "turn", "turn fact", f);
    check(
      f.funding.kind === "operator" && f.funding.id === "env",
      "operator funding attribution",
      f.funding,
    );
    assert.equal(f.status, "reported");
    assert.equal(f.input_tokens, 1000);
    assert.equal(f.output_tokens, 200);
    assert.equal(f.cached_tokens, 400);
    // (1000-400)*1 + 400*1 + 200*2 = 1400 units — cached priced at the
    // input rate (no cached rate configured).
    assert.equal(f.cost_minor, 1400, "priced by fixture rates");
    assert.equal(f.currency, "USD");
    assert.equal(f.cost_basis, "configured_rates");
    assert.equal(f.pricing_revision, "fixture-rates-v1");
    assert.ok(f.quantities.prompt_tokens === 1000, "raw report preserved");
  }
  assert.notEqual(f1.fact_id, f2.fact_id, "each call is its own fact");
  log(
    "facts priced:",
    fs.map((f) => f.cost_minor),
  );

  // --- 2: redelivery replays; a conflicting fact id conflicts --------------
  const replay = await req("POST", `${P}/usage/record`, ptoken, {
    fact_id: f1.fact_id,
    kind: f1.kind,
    phase: f1.phase,
    turn_id: f1.turn_id,
    input_id: f1.input_id,
    round: f1.round ?? 0,
    funding: f1.funding,
    status: "reported",
    input_tokens: 1000,
    output_tokens: 200,
    cached_tokens: 400,
    quantities: {},
  });
  check(
    replay.status === 200 && replay.json.created === false,
    "redelivery replays",
    replay.text,
  );
  const conflict = await req("POST", `${P}/usage/record`, ptoken, {
    fact_id: f1.fact_id,
    kind: "model_call",
    phase: "turn",
    funding: f1.funding,
    status: "reported",
    input_tokens: 1,
    output_tokens: 1,
    quantities: {},
  });
  check(conflict.status === 409, `conflict ${conflict.status}`, conflict.text);
  check((await facts()).length === fs.length, "no duplicated facts");

  // --- 3: memory-phase calls are metered as their own facts ----------------
  const pad = "x".repeat(44 * 1024);
  await say(`memory seed one ${pad}`);
  await say(`memory seed two ${pad}`);
  await waitFor(
    async () => (await facts()).some((f) => f.phase === "memory"),
    "a memory-phase usage fact",
    45_000,
  );
  fs = await facts();
  const memFact = fs.find((f) => f.phase === "memory");
  check(memFact.funding.kind === "operator", "memory fact funding", memFact);
  assert.equal(memFact.status, "reported");
  assert.equal(memFact.cost_minor, 1400);
  const spent = fs.reduce((a, f) => a + (f.cost_minor ?? 0), 0);
  log("memory fact landed; spent so far:", spent);

  // --- 4: a budget denial parks the input without a provider request -------
  // Cap the source at exactly what is already spent: any further call's
  // needed amount exceeds the limit.
  b = await setBudget(spent);
  check(b.status === 200, `tighten budget ${b.status}`, b.text);
  const blockedId = `in-${randomUUID()}`;
  const sub = await req("POST", `${P}/inputs`, ptoken, {
    input_id: blockedId,
    kind: "message",
    payload: { text: "usage blocked three" },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  });
  check(sub.status < 300, `submit blocked ${sub.status}`, sub.text);
  const before = providerRequests();
  await waitFor(
    async () =>
      (await outboxFor(blockedId)).some((o) => o.kind === "budget_wait"),
    "budget_wait outbox entry",
  );
  await sleep(1_000);
  assert.equal(
    providerRequests(),
    before,
    "a denied admission must not reach the provider",
  );
  assert.equal(await inputStatus(blockedId), "waiting");
  log("input parked on budget_wait; provider untouched");

  // --- 5: raising the cap resumes the parked input --------------------------
  b = await setBudget(spent + 1_000_000);
  check(b.status === 200, `raise budget ${b.status}`, b.text);
  await waitFor(
    async () =>
      (await outboxFor(blockedId)).some((o) => o.kind === "turn_completed"),
    "parked input resumes after budget increase",
  );
  assert.equal(await inputStatus(blockedId), "done");
  await waitFor(
    async () => (await facts()).length === fs.length + 1,
    "the resumed call records exactly one more fact",
  );
  log("resumed without duplication");

  // --- 6: a call with no usage report records 'unknown' ---------------------
  writeFileSync(join(DIR, "drop-usage"), "");
  await say("usage mystery four");
  fs = await facts();
  const unknown = fs.at(-1);
  assert.equal(unknown.status, "unknown", "unreported usage is inspectable");
  assert.equal(unknown.input_tokens, null);
  assert.equal(unknown.cost_minor, null, "unknown is never billed as zero");
  assert.ok(providerRequests() > before, "the call really ran");
  rmSync(join(DIR, "drop-usage"));

  // --- summary ---------------------------------------------------------------
  const summary = {
    persona_id: personaId,
    facts: fs.map((f) => ({
      fact_id: f.fact_id,
      phase: f.phase,
      funding: f.funding,
      status: f.status,
      cost_minor: f.cost_minor,
      currency: f.currency,
    })),
    provider_requests: providerRequests(),
  };
  writeFileSync(join(DIR, "summary.json"), JSON.stringify(summary, null, 2));
  log("summary", JSON.stringify(summary, null, 1));
  child.kill("SIGTERM");
  await once(child, "exit");
  svc.kill("SIGTERM");
  log(
    "PASS — usage facts, idempotent record, budget deny/park/resume, unknown usage on real PG + Go + Node",
  );
  process.exit(0);
}
