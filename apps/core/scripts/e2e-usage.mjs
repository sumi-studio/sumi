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
  // the fact then records 'unknown'. A `partial-usage` file sends a usage
  // chunk without completion_tokens.
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
        : existsSync(join(DIR, "partial-usage"))
          ? {
              prompt_tokens: 1000,
              prompt_tokens_details: { cached_tokens: 400 },
            }
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
      // Arms the connection store so a seeded api_key reaches the binding.
      SUMI_MODEL_CONNECTION_KEY: Buffer.alloc(32, 7).toString("base64"),
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
  const setBudget = (limit, kind = "operator", id = "env", rates = {}) =>
    req("PUT", "/internal/dev/usage-budgets", ADMIN, {
      funding_kind: kind,
      funding_id: id,
      limit_minor: limit,
      currency: "USD",
      rate_input_per_mtok: 1_000_000,
      rate_output_per_mtok: 2_000_000,
      pricing_revision: "fixture-rates-v1",
      ...rates,
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
    // Normalized non-overlapping categories: the wire's prompt_tokens
    // (1000) INCLUDES the cached subset (400) — input records 600.
    assert.equal(f.input_tokens, 600);
    assert.equal(f.output_tokens, 200);
    assert.equal(f.cached_tokens, 400);
    // 600*1 + 400*1 + 200*2 = 1400 units — cached priced at the
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
    input_tokens: 600,
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
  // The resumed call records exactly one fact, on its own input — a
  // memory-phase fact may land concurrently, so match the call, not a
  // count.
  await waitFor(
    async () =>
      (await facts()).filter((f) => f.input_id === blockedId).length === 1,
    "the resumed call records its one fact",
  );
  log("resumed without duplication");

  // --- 6: a call with no usage report records 'unknown' ---------------------
  // Under a configured card the unresolvable call keeps its admission
  // estimate as uncertain spend — 'admission_estimate', inspectable,
  // never silently zero and never releasing the hold back as allowance.
  writeFileSync(join(DIR, "drop-usage"), "");
  await say("usage mystery four");
  fs = await facts();
  const unknown = fs.at(-1);
  assert.equal(unknown.status, "unknown", "unreported usage is inspectable");
  assert.equal(unknown.input_tokens, null);
  assert.equal(unknown.cost_basis, "admission_estimate");
  assert.ok(unknown.cost_minor > 0, "the reserved estimate stays spent");
  assert.ok(providerRequests() > before, "the call really ran");
  rmSync(join(DIR, "drop-usage"));

  // --- 6b: a partial usage report stays uncertain until completed ---------
  // The provider reports input but no output. The real wrapper records
  // 'unknown' with the supplied categories and keeps the admission
  // estimate; the HTTP boundary refuses the same partial report as
  // 'reported'; the complete report for the SAME fact supersedes the
  // estimate once.
  writeFileSync(join(DIR, "partial-usage"), "");
  const partialId = await say("usage partial five");
  rmSync(join(DIR, "partial-usage"));
  fs = await facts();
  const partial = fs.find(
    (f) => f.input_id === partialId && f.phase === "turn",
  );
  check(partial, "partial-usage fact recorded", fs);
  assert.equal(partial.status, "unknown", "a partial report is not final");
  assert.equal(partial.input_tokens, 600);
  assert.equal(partial.cached_tokens, 400);
  assert.equal(partial.output_tokens, null, "missing output is not zero");
  assert.equal(partial.cost_basis, "admission_estimate");
  assert.ok(partial.cost_minor > 0, "the estimate stays spent");
  const recordPartial = (body) =>
    req("POST", `${P}/usage/record`, ptoken, {
      fact_id: partial.fact_id,
      kind: partial.kind,
      phase: partial.phase,
      turn_id: partial.turn_id,
      input_id: partial.input_id,
      round: partial.round ?? 0,
      funding: partial.funding,
      quantities: {},
      ...body,
    });
  let pr = await recordPartial({
    status: "reported",
    input_tokens: 600,
    cached_tokens: 400,
  });
  check(pr.status === 400, `partial 'reported' refused ${pr.status}`, pr.text);
  const complete = {
    status: "reported",
    input_tokens: 600,
    output_tokens: 200,
    cached_tokens: 400,
  };
  pr = await recordPartial(complete);
  check(
    pr.status === 200 &&
      pr.json.created === false &&
      pr.json.fact.status === "reported" &&
      pr.json.fact.cost_minor === 1400 &&
      pr.json.fact.cost_basis === "configured_rates",
    "the complete report supersedes the estimate",
    pr.text,
  );
  pr = await recordPartial(complete);
  check(
    pr.status === 200 && pr.json.created === false,
    "complete report replays",
    pr.text,
  );
  pr = await recordPartial({ ...complete, output_tokens: 201 });
  check(
    pr.status === 409,
    `a different report conflicts ${pr.status}`,
    pr.text,
  );
  const settled = (await facts()).filter((f) => f.fact_id === partial.fact_id);
  check(
    settled.length === 1 &&
      settled[0].status === "reported" &&
      settled[0].cost_minor === 1400,
    "one fact carrying only the actual cost",
    settled,
  );
  log("partial usage: estimate", partial.cost_minor, "→ reported 1400");

  // --- 7: a selected connection funds the call that selected it -------------
  // A human-owned api connection — the fact attributes to the connection
  // chosen at call time, not the operator fallback.
  const human = uuidv7();
  let r = await req("POST", "/internal/dev/humans", ADMIN, { human_id: human });
  check(r.status === 201, `seed human ${r.status}`, r.text);
  const connIds = [];
  for (const name of ["fixture-conn-1", "fixture-conn-2"]) {
    r = await req("POST", "/internal/dev/model-connections", ADMIN, {
      human_id: human,
      name,
      preset: "openai-chat",
      base_url: `http://127.0.0.1:${stubPort}`,
      model: "fixture-model",
      api_key: `fixture-key-${name}`,
    });
    check(r.status === 201, `seed connection ${r.status}`, r.text);
    connIds.push(r.json.connection_id);
    b = await setBudget(1_000_000, "connection", r.json.connection_id);
    check(b.status === 200, `seed conn budget ${b.status}`, b.text);
  }
  const [conn1, conn2] = connIds;
  const select = (id) =>
    req("POST", "/internal/dev/model-selections", ADMIN, {
      human_id: human,
      kind: "api",
      connection_id: id,
    });
  r = await select(conn1);
  check(r.status === 200, `select conn1 ${r.status}`, r.text);
  r = await req("POST", `${P}/bind`, ADMIN, { human_id: human });
  check(r.status === 200, `bind persona ${r.status}`, r.text);

  await say("usage conn one");
  fs = await facts();
  const connFact = fs.at(-1);
  assert.equal(connFact.funding.kind, "connection", "connection funding");
  assert.equal(connFact.funding.id, conn1);
  assert.equal(connFact.cost_minor, 1400);
  assert.equal(connFact.currency, "USD");

  // --- 8: a connection switch attributes the next call, never rewrites the old
  r = await select(conn2);
  check(r.status === 200, `select conn2 ${r.status}`, r.text);
  await say("usage conn two");
  fs = await facts();
  const conn2Fact = fs.at(-1);
  assert.equal(conn2Fact.funding.id, conn2, "new call attributes to conn-2");
  assert.equal(
    fs.find((f) => f.fact_id === connFact.fact_id)?.funding.id,
    conn1,
    "the earlier fact keeps its call-time funding",
  );

  // --- 9: denied on conn-2 → switching the selection resumes on conn-1 ------
  b = await setBudget(1, "connection", conn2);
  check(b.status === 200, `tighten conn2 ${b.status}`, b.text);
  const switchedId = `in-${randomUUID()}`;
  r = await req("POST", `${P}/inputs`, ptoken, {
    input_id: switchedId,
    kind: "message",
    payload: { text: "usage switched five" },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  });
  check(r.status < 300, `submit switched ${r.status}`, r.text);
  await waitFor(
    async () =>
      (await outboxFor(switchedId)).some((o) => o.kind === "budget_wait"),
    "budget_wait on conn-2",
  );
  assert.equal(await inputStatus(switchedId), "waiting");
  // The funding change — not a budget raise — resumes the wait: the next
  // attempt admits under conn-1.
  r = await select(conn1);
  check(r.status === 200, `reselect conn1 ${r.status}`, r.text);
  await waitFor(
    async () =>
      (await outboxFor(switchedId)).some((o) => o.kind === "turn_completed"),
    "parked input resumes after the funding change",
  );
  fs = await facts();
  const resumed = fs.at(-1);
  assert.equal(resumed.funding.kind, "connection");
  assert.equal(resumed.funding.id, conn1, "resumed call ran on conn-1");

  // --- 10: a lower rate card under the same limit resumes a parked input ---
  // The owner keeps the limit and cuts the rates. The wait is priced again
  // under the new card: a card that still does not fit leaves the input
  // parked with no provider request; one that fits runs it — no unrelated
  // change and no manual nudge.
  r = await req("POST", "/internal/dev/model-connections", ADMIN, {
    human_id: human,
    name: "fixture-conn-3",
    preset: "openai-chat",
    base_url: `http://127.0.0.1:${stubPort}`,
    model: "fixture-model",
    api_key: "fixture-key-fixture-conn-3",
  });
  check(r.status === 201, `seed connection 3 ${r.status}`, r.text);
  const conn3 = r.json.connection_id;
  b = await setBudget(1, "connection", conn3);
  check(b.status === 200, `tighten conn3 ${b.status}`, b.text);
  r = await select(conn3);
  check(r.status === 200, `select conn3 ${r.status}`, r.text);
  const rateId = `in-${randomUUID()}`;
  r = await req("POST", `${P}/inputs`, ptoken, {
    input_id: rateId,
    kind: "message",
    payload: { text: "usage cheaper six" },
    actor_kind: "human",
    actor_id: "e2e",
    source_surface: "e2e",
  });
  check(r.status < 300, `submit rate ${r.status}`, r.text);
  await waitFor(
    async () => (await outboxFor(rateId)).some((o) => o.kind === "budget_wait"),
    "budget_wait on conn-3",
  );
  const needed = (await outboxFor(rateId)).find((o) => o.kind === "budget_wait")
    .payload.needed_minor;
  // The limit covers 60% of the call at fixture rates.
  const rateLimit = Math.floor(needed * 0.6);
  b = await setBudget(rateLimit, "connection", conn3);
  check(b.status === 200, `conn3 limit ${b.status}`, b.text);
  const beforeRates = providerRequests();
  b = await setBudget(rateLimit, "connection", conn3, {
    rate_input_per_mtok: 750_000,
    rate_output_per_mtok: 1_500_000,
    pricing_revision: "fixture-rates-three-quarter",
  });
  check(b.status === 200, `conn3 three-quarter rates ${b.status}`, b.text);
  await sleep(1_000);
  assert.equal(await inputStatus(rateId), "waiting", "still over the limit");
  assert.equal(providerRequests(), beforeRates, "a parked call sends nothing");
  b = await setBudget(rateLimit, "connection", conn3, {
    rate_input_per_mtok: 100_000,
    rate_output_per_mtok: 200_000,
    pricing_revision: "fixture-rates-tenth",
  });
  check(b.status === 200, `conn3 tenth rates ${b.status}`, b.text);
  await waitFor(
    async () =>
      (await outboxFor(rateId)).some((o) => o.kind === "turn_completed"),
    "parked input resumes after the rate decrease",
  );
  fs = await facts();
  const cheaper = fs.find((f) => f.input_id === rateId && f.phase === "turn");
  check(cheaper, "the resumed call recorded its fact", fs);
  assert.equal(cheaper.funding.id, conn3, "resumed call ran on conn-3");
  // 1000 input * 0.1 + 200 output * 0.2 = 140 under the lowered card.
  assert.equal(cheaper.cost_minor, 140);
  assert.equal(cheaper.pricing_revision, "fixture-rates-tenth");
  log("rate decrease: needed", needed, "limit", rateLimit, "→ resumed at 140");

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
    "PASS — usage facts, idempotent record, budget deny/park/resume, unknown usage, connection funding + switch, funding-change resume on real PG + Go + Node",
  );
  process.exit(0);
}
