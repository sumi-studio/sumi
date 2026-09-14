#!/usr/bin/env node
/**
 * E2E for the memory layer: REAL Go state service + REAL PostgreSQL + real
 * Node core child processes with a deterministic scripted provider
 * (docs/agent/memory-preparation-and-replacement-2026-09-08).
 *
 * Covers:
 *   1. A sealed chunk's L1 branch runs while the conversation continues; a
 *      correction served during preparation still sees the raw original,
 *      and the branch keeps the parent's system prompt and tools.
 *   2. The finished candidate waits below the 40k live limit and is applied
 *      only once the live raw estimate crosses it; the next request renders
 *      the replacement at the chunk's original position, before later raw
 *      records and the correction.
 *   3. SIGKILL while another chunk is preparing → a new process acquires a
 *      new generation → the chunk is prepared again; the same persona keeps
 *      its applied memory, the correction and a contiguous journal, and the
 *      original records stay readable through conversation_history.
 *
 * The scripted provider proves persistence, ordering and recovery. It does
 * not show that a real model writes a faithful replacement or remembers
 * naturally over a long life.
 *
 * It registers its own admin and persona, so it can share a database with the
 * other e2e scripts; state-dev applies migrations on startup.
 *
 *   SUMI_TEST_DB_URL=postgres://… [SUMI_E2E_DIR=<artifacts>] \
 *   [SUMI_E2E_PORT=9521] node scripts/e2e-memory.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  appendFileSync,
  existsSync,
  mkdtempSync,
  openSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
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
  const env = (n) => {
    const v = process.env[n];
    if (!v) throw new Error(`missing env ${n}`);
    return v;
  };
  const dir = env("SUMI_E2E_DIR");
  const gated = process.env.SUMI_BRANCH_GATE === "1";
  // ~11k estimated tokens per reply: each exchange clears the 10k seal.
  const pad = "x".repeat(44 * 1024);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  class ScriptedProvider {
    name = "scripted-memory";
    async *stream(req) {
      appendFileSync(
        join(dir, "requests.jsonl"),
        JSON.stringify({
          pid: process.pid,
          turnId: req.turnId,
          round: req.round,
          tools: req.tools.map((t) => t.name),
          messages: req.messages.map((m) => ({
            role: m.role,
            chars: m.content.length,
            content: m.content.slice(0, 600),
          })),
        }) + "\n",
      );
      const last = req.messages[req.messages.length - 1].content;
      const branch = /^memory-l1-(\d+)$/.exec(req.turnId);
      if (branch) {
        const chunk = Number(branch[1]);
        console.log(`[child ${process.pid}] branch consulted chunk=${chunk}`);
        while (gated && !existsSync(join(dir, `release-${chunk}`))) {
          if (req.signal?.aborted)
            throw new DOMException("aborted", "AbortError");
          await sleep(100);
        }
        const target = JSON.parse(
          last.slice(
            last.indexOf("compact_target\n") + "compact_target\n".length,
          ),
        );
        const said = target.events
          .filter((e) => e.kind === "input_received")
          .map((e) => String(e.payload.text ?? "").slice(0, 60));
        yield {
          type: "text",
          delta: `L1 chunk ${chunk}: the human said ${said.join(" / ")}`,
        };
        yield { type: "done", usage: {} };
        return;
      }
      if (req.round > 0) {
        yield { type: "text", delta: "I read back the stored records." };
        yield { type: "done", usage: {} };
        return;
      }
      const history = /HISTORY: open chunk_seq=(\d+)/.exec(last);
      if (history) {
        yield { type: "text", delta: "Opening the stored records." };
        yield {
          type: "tool_call",
          call: {
            id: "history-0",
            name: "conversation_history",
            arguments: {
              operation: "read",
              chunk_seq: Number(history[1]),
              limit: 5,
            },
          },
        };
        yield { type: "done", usage: {} };
        return;
      }
      const label = last.replace(/^\[\w+\] /, "").slice(0, 24);
      yield { type: "text", delta: `ack ${label} ${pad}` };
      yield { type: "done", usage: {} };
    }
  }

  const secretary = new Secretary({
    personaId: env("SUMI_PERSONA_ID"),
    // A restarted process is a different holder, as on an ordinary host
    // restart: it waits out the dead holder's lease, then takes a new
    // generation.
    holderId: `memory-e2e-${process.pid}`,
    state: new HttpStateClient(
      env("SUMI_STATE_URL"),
      env("SUMI_PERSONA_TOKEN"),
    ),
    provider: new ScriptedProvider(),
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
    console.error("e2e-memory: SUMI_TEST_DB_URL required — real PostgreSQL");
    process.exit(2);
  }
  // Artifacts (request log, child and state-dev logs, summary) default to a
  // disposable directory; SUMI_E2E_DIR keeps them somewhere durable.
  const DIR =
    process.env.SUMI_E2E_DIR ??
    mkdtempSync(join(tmpdir(), "sumi-e2e-memory-run-"));
  process.env.SUMI_E2E_DIR = DIR;
  const API_DIR = resolve(import.meta.dirname, "../../api");
  const SELF = resolve(import.meta.dirname, "e2e-memory.mjs");
  const PORT = Number(process.env.SUMI_E2E_PORT ?? 9521);
  const BASE = `http://127.0.0.1:${PORT}`;
  const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;
  const children = [];

  const log = (...a) => console.log("[e2e-memory]", ...a);
  const fail = (msg, evidence) => {
    console.error("[e2e-memory] FAIL:", msg);
    if (evidence !== undefined)
      console.error(JSON.stringify(evidence, null, 1));
    for (const c of children) c.kill("SIGKILL");
    process.exit(1);
  };
  const assert = (cond, msg, evidence) => {
    if (!cond) fail(msg, evidence);
  };
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

  const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-memory-"));
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
    display_name: "e2e memory secretary",
  });
  assert(
    created.status === 201,
    `createPersona ${created.status}`,
    created.text,
  );
  const ptoken = created.json.persona_token;

  const P = `/internal/core/personas/${personaId}`;
  const mem = async () => (await req("GET", `${P}/memory`, ptoken)).json;
  const events = async () =>
    (await req("GET", `${P}/events?after_seq=0&limit=1000`, ptoken)).json
      .events;
  const outboxFor = async (inputId) =>
    (await req("GET", `${P}/outbox?after_seq=0`, ptoken)).json.outbox.filter(
      (o) => o.payload.input_id === inputId,
    );
  const requests = () =>
    existsSync(join(DIR, "requests.jsonl"))
      ? readFileSync(join(DIR, "requests.jsonl"), "utf8")
          .split("\n")
          .filter(Boolean)
          .map((l) => JSON.parse(l))
      : [];
  const turnRequestFor = (text) =>
    requests()
      .filter(
        (r) =>
          !r.turnId.startsWith("memory-l1-") &&
          r.round === 0 &&
          r.messages[r.messages.length - 1].content.includes(text),
      )
      .at(-1);
  async function waitFor(fn, what, timeoutMs = 30_000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (await fn()) return;
      if (Date.now() > deadline) {
        fail(`timed out waiting for ${what}`, {
          memory: await mem(),
          events: (await events()).map((e) => `${e.seq}:${e.kind}`),
        });
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
    assert(r.status >= 200 && r.status < 300, `submit ${r.status}`, r.text);
    await waitFor(
      async () => (await outboxFor(inputId)).length > 0,
      `reply to "${text.slice(0, 32)}"`,
    );
    log("replied:", text.slice(0, 48));
    return inputId;
  }
  const spawnChild = (n, extra) => {
    const out = openSync(join(DIR, `child-${n}.log`), "a");
    const proc = spawn(process.execPath, [SELF, "--child"], {
      env: {
        ...process.env,
        SUMI_STATE_URL: BASE,
        SUMI_PERSONA_ID: personaId,
        SUMI_PERSONA_TOKEN: ptoken,
        ...extra,
      },
      stdio: ["ignore", out, out],
    });
    children.push(proc);
    log(`child ${n} pid=${proc.pid}`);
    return proc;
  };
  const generationsOf = (n) =>
    [
      ...readFileSync(join(DIR, `child-${n}.log`), "utf8").matchAll(
        /writer acquired \{"generation":(\d+)/g,
      ),
    ].map((m) => Number(m[1]));
  const isFragment = (m, chunk) =>
    m.content.startsWith("[Memory fragment") &&
    m.content.includes(`L1 chunk ${chunk}: the human said`);

  // --- 1: preparation runs alongside the conversation ----------------------
  let child = spawnChild(1, { SUMI_BRANCH_GATE: "1" });
  await say("COLOR=amber is my favorite color");
  await say("second topic: the train schedule");
  await waitFor(async () => (await mem()).preparing === 1, "chunk 1 preparing");
  log("chunk 1 preparing; sending a correction while the branch waits");
  await say("CORRECTION=violet: I said amber, but it is violet");
  const during = turnRequestFor("CORRECTION=violet");
  assert(
    during?.messages.some((m) => /^\[human\] COLOR=amber/.test(m.content)),
    "the correction turn must see the raw original while chunk 1 prepares",
    during,
  );
  const branch1 = requests().find((r) => r.turnId === "memory-l1-1");
  assert(
    branch1 && branch1.messages[0].content === during.messages[0].content,
    "branch keeps the parent's system prompt",
    branch1?.messages[0],
  );
  assert(
    JSON.stringify(branch1.tools) === JSON.stringify(during.tools),
    "branch keeps the parent's tool definitions",
    { branch: branch1.tools, parent: during.tools },
  );
  writeFileSync(join(DIR, "release-1"), "");
  await waitFor(
    async () => (await mem()).prepared + (await mem()).applied >= 1,
    "chunk 1 prepared",
  );
  const shelved = await mem();
  assert(
    shelved.applied === 0 &&
      shelved.live_raw_tokens <= shelved.live_limit_tokens,
    "the prepared candidate waits while live raw is at or under 40k",
    shelved,
  );
  log("chunk 1 shelved", shelved);

  // --- 2: application after crossing 40k, in place -------------------------
  await say("fourth: dinner plans for Friday");
  await say("fifth: the weekend trip");
  await waitFor(
    async () => (await mem()).applied >= 1,
    "chunk 1 applied after live raw crosses 40k",
  );
  const applied = await mem();
  log("applied", applied);
  await say("sixth: which color do I like now?");
  const after = turnRequestFor("sixth: which color");
  const at = (pred) => after.messages.findIndex(pred);
  const frag = at((m) => isFragment(m, 1));
  const m2 = at((m) => /^\[human\] second topic/.test(m.content));
  const corr = at((m) => /^\[human\] CORRECTION=violet/.test(m.content));
  assert(
    frag > 0 && frag < m2 && m2 < corr,
    "replacement renders at chunk 1's original position, before later raw records and the correction",
    {
      frag,
      m2,
      corr,
      roles: after.messages.map((m) => m.content.slice(0, 60)),
    },
  );
  assert(
    !after.messages.some((m) => /^\[human\] COLOR=amber/.test(m.content)),
    "applied originals no longer render raw",
  );

  // --- 3: crash during preparation, restart, same life ---------------------
  await waitFor(
    async () => (await mem()).preparing === 1,
    "chunk 2 preparing behind its gate",
  );
  const beforeKill = await events();
  log("SIGKILL child 1 while chunk 2 prepares");
  child.kill("SIGKILL");
  await once(child, "exit");
  child = spawnChild(2, { SUMI_BRANCH_GATE: "0" });
  await waitFor(
    async () =>
      requests().some((r) => r.turnId === "memory-l1-2" && r.pid === child.pid),
    "chunk 2 prepared again by the restarted process",
    45_000,
  );
  await waitFor(
    async () => {
      const s = await mem();
      return s.preparing === 0 && s.sealed === 0;
    },
    "memory shelf settles after restart",
    45_000,
  );
  await say("HISTORY: open chunk_seq=1");
  const hist = turnRequestFor("HISTORY: open chunk_seq=1");
  assert(
    hist.messages.some((m) => isFragment(m, 1)),
    "applied memory still renders after the restart",
    hist.messages.map((m) => m.content.slice(0, 80)),
  );
  assert(
    hist.messages.some((m) => m.content.includes("CORRECTION=violet")),
    "the correction is still in the sent context after the restart",
  );
  const finalEvents = await events();
  const historyResult = finalEvents
    .filter(
      (e) =>
        e.kind === "tool_result" && e.payload.tool === "conversation_history",
    )
    .at(-1);
  assert(
    historyResult &&
      JSON.stringify(historyResult.payload.response ?? {}).includes(
        "COLOR=amber",
      ),
    "conversation_history opens chunk 1's original record after application",
    historyResult,
  );
  const seqs = finalEvents.map((e) => e.seq);
  assert(
    seqs.every((s, i) => s === i + 1),
    "one contiguous journal across the crash",
    seqs,
  );
  assert(
    beforeKill.every(
      (e, i) => finalEvents[i].seq === e.seq && finalEvents[i].kind === e.kind,
    ),
    "pre-crash records are unchanged after restart",
  );
  const g1 = generationsOf(1);
  const g2 = generationsOf(2);
  assert(
    g1.length === 1 && g2.length === 1 && g2[0] > g1[0],
    "restart took a new generation",
    { g1, g2 },
  );

  child.kill("SIGTERM");
  await once(child, "exit");
  const summary = {
    persona_id: personaId,
    generations: { before_crash: g1[0], after_restart: g2[0] },
    memory_when_shelved: shelved,
    memory_after_apply: applied,
    memory_final: await mem(),
    journal_records: finalEvents.length,
    branch_requests: requests()
      .filter((r) => r.turnId.startsWith("memory-l1-"))
      .map((r) => ({ turnId: r.turnId, pid: r.pid })),
  };
  writeFileSync(join(DIR, "summary.json"), JSON.stringify(summary, null, 2));
  log("summary", JSON.stringify(summary));
  log(
    "PASS — memory preparation, application and restart on real PG + real Go + real Node",
  );
  svc.kill("SIGTERM");
  process.exit(0);
}
