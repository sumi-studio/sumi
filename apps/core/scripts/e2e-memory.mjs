#!/usr/bin/env node
/**
 * E2E for the memory layer: REAL Go state service + REAL PostgreSQL + the
 * REAL workspace file service + real Node core child processes. The model is
 * scripted: ordinary turns answer with a padded reply, and memory branches
 * run the private-workspace protocol through memoryAgentRound
 * (memory-e2e-support.mjs) — read the frozen source, write a draft, reread
 * that version and the source, review, confirm in a later round.
 *
 * Covers:
 *   1. A sealed chunk is claimed automatically at an actual model boundary:
 *      the branch's frozen prefix is exactly what a parent request sent (plus
 *      the reply it received), with the same tool definitions, and its
 *      rounds run while the conversation continues — a correction served
 *      meanwhile still sees the raw original.
 *   2. SIGKILL while the branch holds its review round, after its draft was
 *      checkpointed → a new process takes a new generation and continues the
 *      durable transcript: same prefix, the killed process's draft, no new
 *      write — review and confirm only.
 *   3. The confirmed draft waits below the 40k live limit and is applied only
 *      once the live raw estimate crosses it; the next request renders it at
 *      chunk 1's original position with every later message still raw after
 *      it, and conversation_history opens the original records. The journal
 *      stays contiguous across the crash, and the branch never reached the
 *      workspace file service or journaled a tool call.
 *
 * The file service is real because the state service advertises file.read
 * and file.write only when one is configured; the branch's own file
 * operations are private checkpoints and must never reach it.
 *
 * The scripted model proves persistence, ordering and recovery. It does not
 * show that a real model writes a faithful replacement or remembers
 * naturally over a long life.
 *
 * It registers its own admin and persona. SUMI_TEST_DB_URL must be a
 * disposable database: state-dev applies migrations there and the file
 * service binds its canonical root to it (a stable temp directory per
 * database URL), as a local install shares one database.
 *
 *   SUMI_TEST_DB_URL=postgres://… [SUMI_E2E_DIR=<artifacts>] \
 *   [SUMI_E2E_PORT=9521] node scripts/e2e-memory.mjs
 *   (the file service listens on SUMI_E2E_PORT+1)
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import {
  appendFileSync,
  existsSync,
  mkdtempSync,
  openSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import {
  memoryAgentRound,
  memoryBranchView,
  memoryPathsInWorkspace,
  messageDigest,
  startFileService,
} from "./memory-e2e-support.mjs";


const CHILD = process.argv.includes("--child");
const SELFTEST = process.argv.includes("--selftest");
if (CHILD) {
  await childMain();
} else if (SELFTEST) {
  requestLogSelfTest();
} else {
  await main();
}

// --------------------------------------------------------- request log ---
// Each child appends one JSON record plus a newline per provider call to its
// own requests-<pid>.jsonl through a single appendFileSync write. The newline
// is therefore the commit marker: every newline-terminated line is one
// complete record and is parsed strictly, so corrupt content still fails.
// A final segment without a trailing newline can only be an append still in
// flight or residue from a writer SIGKILLed mid-write — never a completed
// record — so it is dropped and becomes visible once the write finishes.
// Per-writer files mean a dead writer's torn tail can never merge onto a
// later record from another process.
function parseRequestLog(text) {
  const lines = text.split("\n");
  if (!text.endsWith("\n")) lines.pop();
  return lines.filter(Boolean).map((l) => JSON.parse(l));
}

function requestLogSelfTest() {
  const record = (over) =>
    JSON.stringify({
      pid: 1234,
      turnId: "t-1",
      round: 0,
      tools: [],
      messages: [],
      ...over,
    });
  const a = record({ turnId: "t-a" });
  const b = record({ turnId: "t-b" });
  const check = (name, cond) => {
    if (!cond) {
      console.error(`[e2e-memory] SELFTEST FAIL: ${name}`);
      process.exit(1);
    }
    console.log(`[e2e-memory] selftest ok: ${name}`);
  };
  check(
    "parses complete records",
    parseRequestLog(`${a}\n${b}\n`).length === 2,
  );
  check(
    "drops an unterminated in-flight tail",
    parseRequestLog(`${a}\n${b.slice(0, 40)}`).length === 1,
  );
  check(
    "drops a killed writer's torn residue and keeps complete records",
    (() => {
      const got = parseRequestLog(`${a}\n${b.slice(0, 25)}`);
      return got.length === 1 && got[0].turnId === "t-a";
    })(),
  );
  for (const [name, text] of [
    ["mid-file corrupt record", `${a}\n{"pid":"tampered"xxx}\n`],
    ["corrupt terminated tail", `${a}\nnot json\n`],
    [
      "corrupt prefix before a completed record",
      `${a}\nnot-json-before-a-completed-record${b}\n`,
    ],
    ["non-record boundary shape", `${a}\n${b.slice(0, 25)}{"pid":5}\n`],
  ]) {
    let threw = false;
    try {
      parseRequestLog(text);
    } catch {
      threw = true;
    }
    check(`genuine corruption still fails: ${name}`, threw);
  }
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
  // A branch of chunk >= SUMI_BRANCH_HOLD_FROM holds its SUMI_BRANCH_HOLD
  // stage's model call open until DIR gains release-<chunk>.
  const holdStage = process.env.SUMI_BRANCH_HOLD ?? "";
  const holdFrom = Number(process.env.SUMI_BRANCH_HOLD_FROM ?? 1);
  // ~11k estimated tokens per reply: each exchange clears the 10k seal.
  const pad = "x".repeat(44 * 1024);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  class ScriptedProvider {
    name = "scripted-memory";
    async *stream(req) {
      const view =
        req.phase === "memory" ? memoryBranchView(req.messages) : null;
      if (req.phase === "memory" && !view)
        throw new Error("memory request without its branch instruction");
      const decision = view
        ? memoryAgentRound(view, { label: `pid ${process.pid}` })
        : null;
      appendFileSync(
        join(dir, `requests-${process.pid}.jsonl`),
        JSON.stringify({
          pid: process.pid,
          turnId: req.turnId,
          phase: req.phase ?? "turn",
          round: req.round,
          chunk: view?.chunk,
          stage: decision?.stage,
          tools: req.tools.map((t) => t.name),
          digests: req.messages.map(messageDigest),
          messages: req.messages.map((m) => ({
            role: m.role,
            chars: m.content.length,
            content: m.content.slice(0, 600),
            ...(m.toolCalls
              ? {
                  toolCalls: m.toolCalls.map((c) => ({
                    name: c.name,
                    arguments: c.arguments,
                  })),
                }
              : {}),
          })),
          ...(decision
            ? { decision: { text: decision.text, calls: decision.calls } }
            : {}),
        }) + "\n",
      );
      const last = req.messages[req.messages.length - 1].content;
      if (view) {
        console.log(
          `[child ${process.pid}] branch chunk=${view.chunk} round=${req.round} stage=${decision.stage}`,
        );
        while (
          decision.stage === holdStage &&
          view.chunk >= holdFrom &&
          !existsSync(join(dir, `release-${view.chunk}`))
        ) {
          if (req.signal?.aborted)
            throw new DOMException("aborted", "AbortError");
          await sleep(100);
        }
        if (decision.text) yield { type: "text", delta: decision.text };
        for (const call of decision.calls)
          yield { type: "tool_call", call: { ...call, route: "normal" } };
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
            route: "normal",
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
      const label = last
        .replace(/^\[Received [^\]]*\]\n/, "")
        .replace(/^\[\w+\] /, "")
        .slice(0, 24);
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
  // Parser contract checks run on every invocation — including CI — so the
  // framing rules and their negative controls cannot silently regress.
  requestLogSelfTest();
  const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
  if (!DB_URL) {
    console.error("e2e-memory: SUMI_TEST_DB_URL required — real PostgreSQL");
    process.exit(2);
  }
  // Artifacts (request log, child and service logs, summary) default to a
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
  let passed = 0;
  const fail = (msg, evidence) => {
    console.error("[e2e-memory] FAIL:", msg);
    if (evidence !== undefined)
      console.error(JSON.stringify(evidence, null, 1));
    for (const c of children) c.kill("SIGKILL");
    process.exit(1);
  };
  const assert = (cond, msg, evidence) => {
    if (!cond) fail(msg, evidence);
    passed++;
    log(`ok ${passed} - ${msg}`);
  };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  process.on("exit", () => {
    for (const c of children) c.kill("SIGKILL");
  });

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

  log("starting the workspace file service…");
  const files = await startFileService({
    dbUrl: DB_URL,
    port: PORT + 1,
    outDir: binDir,
    children,
  });

  log("starting state-dev on", BASE);
  const svcOut = openSync(join(DIR, "state-dev.log"), "a");
  const svc = spawn(bin, [], {
    env: {
      ...process.env,
      ...files.env,
      SUMI_DB_URL: DB_URL,
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
    },
    stdio: ["ignore", svcOut, svcOut],
  });
  children.push(svc);
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
  if (created.status !== 201) fail(`createPersona ${created.status}`, created.text);
  const ptoken = created.json.persona_token;

  const P = `/internal/core/personas/${personaId}`;
  const tools = (await req("GET", `${P}/tools`, ptoken)).json?.tools ?? [];
  assert(
    tools.includes("file.read") && tools.includes("file.write"),
    "the state service advertises the workspace file tools",
    tools,
  );
  const mem = async () => (await req("GET", `${P}/memory`, ptoken)).json;
  const events = async () =>
    (await req("GET", `${P}/events?after_seq=0&limit=1000`, ptoken)).json
      .events;
  const outboxFor = async (inputId) =>
    (await req("GET", `${P}/outbox?after_seq=0`, ptoken)).json.outbox.filter(
      (o) => o.payload.input_id === inputId,
    );
  const requests = () =>
    readdirSync(DIR)
      .filter((f) => /^requests-\d+\.jsonl$/.test(f))
      .flatMap((f) => parseRequestLog(readFileSync(join(DIR, f), "utf8")));
  const turnRequests = () => requests().filter((r) => r.phase !== "memory");
  const branchRequests = (chunk, pid) =>
    requests()
      .filter(
        (r) =>
          r.phase === "memory" &&
          r.chunk === chunk &&
          (pid === undefined || r.pid === pid),
      )
      .sort((a, b) => a.round - b.round);
  const turnRequestFor = (text) =>
    turnRequests()
      .filter(
        (r) =>
          r.round === 0 &&
          r.messages[r.messages.length - 1].content.includes(text),
      )
      .at(-1);
  /** The frozen parent prefix of a branch request: everything before the
   * branch instruction. */
  const prefixOf = (r) =>
    r.digests.slice(0, memoryBranchView(r.messages).prefix.length);
  async function waitFor(fn, what, timeoutMs = 30_000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      if (await fn()) return;
      if (Date.now() > deadline) {
        fail(`timed out waiting for ${what}`, {
          memory: await mem(),
          events: (await events()).map((e) => `${e.seq}:${e.kind}`),
          branch: requests()
            .filter((r) => r.phase === "memory")
            .map((r) => `${r.pid} chunk ${r.chunk} r${r.round} ${r.stage}`),
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
    if (r.status < 200 || r.status >= 300) fail(`submit ${r.status}`, r.text);
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
  const raw = (m, text) =>
    new RegExp(`^(?:\\[Received [^\\]]*\\]\\n)?\\[human\\] ${text}`).test(
      m.content,
    );
  const writesIn = (r) =>
    r.messages.flatMap((m) =>
      (m.toolCalls ?? []).filter((c) => c.name === "file.write"),
    );

  // --- 1: automatic claim at an actual boundary; branch alongside turns -----
  let child = spawnChild(1, { SUMI_BRANCH_HOLD: "review" });
  const pid1 = child.pid;
  await say("COLOR=amber is my favorite color");
  await say("second topic: the train schedule");
  await waitFor(
    async () => (await mem()).preparing === 1,
    "chunk 1 claimed without any explicit trigger",
  );
  await waitFor(
    async () => branchRequests(1).some((r) => r.stage === "review"),
    "chunk 1's branch reaches its review round",
  );
  const before = branchRequests(1, pid1);
  const stages1 = before.map((r) => r.stage);
  assert(
    stages1.filter((s) => s === "read_source").length >= 2 &&
      stages1.indexOf("write") === stages1.lastIndexOf("write") &&
      stages1.indexOf("write") > stages1.lastIndexOf("read_source") &&
      stages1.indexOf("reread") > stages1.indexOf("write") &&
      stages1.at(-1) === "review",
    "the branch paged the source, wrote one draft, reread it and the source, then asked for review",
    stages1,
  );
  const prefix = prefixOf(before[0]);
  const parent = turnRequests().find(
    (t) =>
      JSON.stringify(t.digests) === JSON.stringify(prefix) ||
      (JSON.stringify(t.digests) === JSON.stringify(prefix.slice(0, -1)) &&
        before[0].messages[prefix.length - 1].role === "assistant"),
  );
  assert(
    parent && prefix.length > 2,
    "the branch prefix is exactly an actual parent request (and the reply it received)",
    { prefix, turns: turnRequests().map((t) => t.digests) },
  );
  assert(
    before.every(
      (r) =>
        JSON.stringify(prefixOf(r)) === JSON.stringify(prefix) &&
        JSON.stringify(r.tools) === JSON.stringify(parent.tools),
    ),
    "every branch round keeps that frozen prefix and the parent's tool definitions",
  );
  log("chunk 1 held at review; sending a correction while the branch waits");
  await say("CORRECTION=violet: I said amber, but it is violet");
  const during = turnRequestFor("CORRECTION=violet");
  assert(
    during?.messages.some((m) => raw(m, "COLOR=amber")),
    "the correction turn sees the raw original while chunk 1 prepares",
    during?.messages.map((m) => m.content.slice(0, 60)),
  );
  const heldShape = await mem();
  assert(
    heldShape.preparing === 1 && heldShape.prepared === 0,
    "nothing is prepared while the branch waits for its review",
    heldShape,
  );

  // --- 2: crash mid-branch; the durable transcript continues ---------------
  const beforeKill = await events();
  log("SIGKILL child 1 while chunk 1's review round is in flight");
  child.kill("SIGKILL");
  await once(child, "exit");
  // The restarted process runs chunk 1 freely and holds later chunks at
  // review, so only chunk 1 can be applied below.
  writeFileSync(join(DIR, "release-1"), "");
  child = spawnChild(2, { SUMI_BRANCH_HOLD: "review", SUMI_BRANCH_HOLD_FROM: "2" });
  const pid2 = child.pid;
  await waitFor(
    async () => branchRequests(1, pid2).some((r) => r.stage === "confirm"),
    "the restarted process confirms chunk 1",
    45_000,
  );
  await waitFor(
    async () => (await mem()).prepared + (await mem()).applied >= 1,
    "chunk 1 prepared",
  );
  const resumed = branchRequests(1, pid2);
  assert(
    JSON.stringify(resumed.map((r) => r.stage)) ===
      JSON.stringify(["review", "confirm"]),
    "the restarted branch continued at review, without reading or writing again",
    resumed.map((r) => r.stage),
  );
  assert(
    JSON.stringify(prefixOf(resumed[0])) === JSON.stringify(prefix),
    "the restarted branch has the same frozen prefix",
  );
  const restoredWrites = writesIn(resumed[0]);
  assert(
    restoredWrites.length === 1 &&
      restoredWrites[0].arguments.content_text.includes(`draft by pid ${pid1}`),
    "the restarted branch's transcript carries the killed process's durable draft",
    restoredWrites,
  );
  const shelved = await mem();
  assert(
    shelved.applied === 0 &&
      shelved.live_raw_tokens <= shelved.live_limit_tokens,
    "the confirmed replacement waits while live raw is at or under 40k",
    shelved,
  );

  // --- 3: application after crossing 40k, in place -------------------------
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
  const frag = at(
    (m) =>
      m.content.startsWith("[Memory fragment") &&
      m.content.includes("L1 chunk 1 ("),
  );
  const order = [
    frag,
    at((m) => raw(m, "second topic")),
    at((m) => raw(m, "CORRECTION=violet")),
    at((m) => raw(m, "fourth")),
    at((m) => raw(m, "fifth")),
    after.messages.length - 1,
  ];
  assert(
    frag > 0 && order.every((v, i) => i === 0 || v > order[i - 1]),
    "the replacement renders at chunk 1's original position; every later message stays raw after it",
    { order, messages: after.messages.map((m) => m.content.slice(0, 60)) },
  );
  assert(
    after.messages[frag].content.includes(`draft by pid ${pid1}`) &&
      after.messages[frag].content.includes("COLOR=amber"),
    "the applied text is the draft written before the crash",
    after.messages[frag].content,
  );
  assert(
    !after.messages.some((m) => raw(m, "COLOR=amber")),
    "applied originals no longer render raw",
  );

  await say("HISTORY: open chunk_seq=1");
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
  assert(
    !finalEvents.some(
      (e) => e.kind === "tool_call" && String(e.payload.tool).startsWith("file."),
    ) && memoryPathsInWorkspace(files.root).length === 0,
    "branch file operations never reached the journal or the workspace file service",
    memoryPathsInWorkspace(files.root),
  );
  const branchTranscripts = requests().filter((r) => r.phase === "memory");
  assert(
    !branchTranscripts.some((r) =>
      r.messages.some((m) => m.content.includes("memory_control_error")),
    ),
    "no branch round was refused by the protocol",
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
    branch_rounds: branchTranscripts.map(
      (r) => `pid ${r.pid} chunk ${r.chunk} round ${r.round}: ${r.stage}`,
    ),
    memory_while_held: heldShape,
    memory_when_shelved: shelved,
    memory_after_apply: applied,
    memory_final: await mem(),
    fragment: after.messages[frag].content,
    journal_records: finalEvents.length,
    checks: passed,
  };
  writeFileSync(join(DIR, "summary.json"), JSON.stringify(summary, null, 2));
  log("summary", JSON.stringify(summary));
  log(
    `PASS — ${passed} checks: automatic memory branch, durable restart and in-place application on real PG + real Go + real filesvc + real Node`,
  );
  svc.kill("SIGTERM");
  files.proc.kill("SIGTERM");
  process.exit(0);
}
