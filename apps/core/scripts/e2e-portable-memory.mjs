#!/usr/bin/env node
/**
 * E2E: a secretary's durable memory crosses a Local→Cloud transfer.
 *
 * Two REAL Go state services (state-dev, which mounts the transfer routes)
 * on two separate REAL PostgreSQL databases, driven by the real Node core
 * with a deterministic scripted provider. The provider proves state
 * continuity — carried fragments, fencing, lifecycle resumption — not model
 * recall quality.
 *
 *   1. Local: exchanges big enough to seal several chunks. Chunk 1 is
 *      prepared and applied once live raw crosses the limit; chunk 2 is
 *      claimed and its preparation branch is still in flight (held open by
 *      a release-file gate) when the transfer seals.
 *   2. The seal fences the writer; a 'preparing' claim cannot cross, so the
 *      cut normalizes chunk 2 back to 'sealed'. The bundle carries every
 *      chunk row: ranges, chunk_seq locators, the applied replacement text,
 *      and lifecycle state.
 *   3. Cloud stages and activates. Its first turn already renders chunk 1's
 *      accepted fragment at the original journal position — before any new
 *      model call — and the settled chunk is never re-prepared there.
 *   4. Cloud's own writer resumes the carried shelf: the normalized chunk
 *      is claimed under the destination's generation and prepared there;
 *      conversation_history still opens the carried chunk's originals.
 *      Local stays sealed and its memory rows unchanged.
 *
 * Requires SUMI_TEST_DB_URL on a server that allows CREATE DATABASE.
 * SUMI_E2E_DIR (optional) keeps child/request logs and the bundle.
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:55432/sumi?sslmode=disable \
 *     node scripts/e2e-portable-memory.mjs
 */
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
// One scripted core per side. The provider answers ordinary turns with a
// padded reply (~11k estimated tokens, so each exchange seals one chunk) and
// answers memory branches with a deterministic fragment. SUMI_BRANCH_GATE=1
// holds a branch's model call open until the side's DIR gains release-N.
async function childMain() {
  const { Secretary } = await import("../src/secretary.ts");
  const { HttpStateClient } = await import("../src/state-client.ts");
  const env = (n) => {
    const v = process.env[n];
    if (!v) throw new Error(`missing env ${n}`);
    return v;
  };
  const dir = env("SUMI_SIDE_DIR");
  const reqLog = join(dir, "requests.jsonl");
  const gated = process.env.SUMI_BRANCH_GATE === "1";
  const pad = "y".repeat(44 * 1024);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  class ScriptedProvider {
    name = "scripted-portable-memory";
    async *stream(req) {
      appendFileSync(
        reqLog,
        JSON.stringify({
          pid: process.pid,
          turnId: req.turnId,
          round: req.round,
          messages: req.messages.map((m) => ({
            role: m.role,
            chars: m.content.length,
            content: m.content.slice(0, 700),
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
    holderId: `portable-mem-${process.env.SUMI_SIDE ?? "side"}-${process.pid}`,
    state: new HttpStateClient(env("SUMI_STATE_URL"), env("SUMI_PERSONA_TOKEN")),
    provider: new ScriptedProvider(),
    leaseTtlMs: 3_000,
    renewEveryMs: 1_000,
    contextLimit: 5_000,
    pollIntervalMs: 100,
    scheduleEveryMs: 1_000,
    idgen: () => crypto.randomUUID(),
    log: (msg, fields) =>
      console.log(`[child ${process.pid}] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });
  const ac = new AbortController();
  process.on("SIGTERM", () => ac.abort());
  await secretary.run(ac.signal);
}

// --------------------------------------------------------------- parent ---
async function main() {
  const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
  if (!DB_URL) {
    console.error("e2e-portable-memory: SUMI_TEST_DB_URL required — real PostgreSQL");
    process.exit(2);
  }
  const DIR = process.env.SUMI_E2E_DIR ?? mkdtempSync(join(tmpdir(), "sumi-e2e-portmem-"));
  const LOCAL_DIR = join(DIR, "local");
  const CLOUD_DIR = join(DIR, "cloud");
  mkdirSync(LOCAL_DIR, { recursive: true });
  mkdirSync(CLOUD_DIR, { recursive: true });
  const API_DIR = resolve(import.meta.dirname, "../../api");
  const SELF = resolve(import.meta.dirname, "e2e-portable-memory.mjs");
  const run = randomUUID().replaceAll("-", "").slice(0, 10);
  const DB_PREFIX = process.env.SUMI_PORTABLE_DB_PREFIX ?? "sumi_portable_repair";
  const LOCAL_DB = withDatabase(DB_URL, `${DB_PREFIX}_memlocal_${run}`);
  const CLOUD_DB = withDatabase(DB_URL, `${DB_PREFIX}_memcloud_${run}`);
  const LOCAL_ADMIN = `local-admin-${randomUUID()}`;
  const CLOUD_ADMIN = `cloud-admin-${randomUUID()}`;
  const LOCAL_PORT = Number(process.env.SUMI_PORTABLE_LOCAL_PORT ?? 9440);
  const CLOUD_PORT = Number(process.env.SUMI_PORTABLE_CLOUD_PORT ?? 9441);
  const LOCAL = `http://127.0.0.1:${LOCAL_PORT}`;
  const CLOUD = `http://127.0.0.1:${CLOUD_PORT}`;
  const TRANSFER = `e2e-mem-move-${run}`;

  let passed = 0;
  const children = [];
  process.on("exit", () => {
    for (const c of children) c.kill("SIGKILL");
  });
  const log = (...a) => console.log("[portable-memory]", ...a);
  const check = (cond, msg, detail) => {
    if (!cond) {
      console.error(`[portable-memory] not ok - ${msg}`);
      if (detail !== undefined)
        console.error(
          "[portable-memory] detail:",
          typeof detail === "string" ? detail : JSON.stringify(detail, null, 1),
        );
      process.exit(1);
    }
    passed++;
    console.log(`[portable-memory] ok ${passed} - ${msg}`);
  };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  function withDatabase(url, name) {
    const q = url.indexOf("?");
    const base = q >= 0 ? url.slice(0, q) : url;
    return base.slice(0, base.lastIndexOf("/") + 1) + name + (q >= 0 ? url.slice(q) : "");
  }

  async function req(base, method, path, token, body, ndjson = false) {
    const res = await fetch(base + path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": ndjson ? "application/x-ndjson" : "application/json",
      },
      body: body === undefined ? undefined : ndjson ? body : JSON.stringify(body),
    });
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      /* ndjson or empty */
    }
    return { status: res.status, json, text };
  }

  async function waitFor(label, fn, timeoutMs = 40_000) {
    const deadline = Date.now() + timeoutMs;
    let last;
    while (Date.now() < deadline) {
      last = await fn();
      if (last === true) return;
      await sleep(250);
    }
    check(false, `timed out waiting: ${label}`, last);
  }

  function uuidv7() {
    const now = Date.now().toString(16).padStart(12, "0");
    const r = randomUUID().replaceAll("-", "");
    return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
  }

  function startService(label, dbUrl, port, admin) {
    const proc = spawn(bin, [], {
      env: {
        ...process.env,
        SUMI_DB_URL: dbUrl,
        SUMI_DB_CREATE: "1",
        SUMI_CORE_STATE_TOKEN: admin,
        SUMI_STATE_LISTEN: `127.0.0.1:${port}`,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    proc.stderr.on("data", (d) => process.stderr.write(`[${label}] ${d}`));
    children.push(proc);
    return proc;
  }

  function spawnChild(side, base, token, personaId, gated) {
    const out = openSync(join(DIR, `child-${side}.log`), "a");
    const proc = spawn(process.execPath, [SELF, "--child"], {
      env: {
        ...process.env,
        SUMI_STATE_URL: base,
        SUMI_PERSONA_ID: personaId,
        SUMI_PERSONA_TOKEN: token,
        SUMI_SIDE: side,
        SUMI_SIDE_DIR: side === "local" ? LOCAL_DIR : CLOUD_DIR,
        SUMI_BRANCH_GATE: gated ? "1" : "0",
      },
      stdio: ["ignore", out, out],
    });
    children.push(proc);
    const exited = new Promise((res) => proc.on("exit", (code, signal) => res({ code, signal })));
    return { proc, exited, output: () => readFileSync(join(DIR, `child-${side}.log`), "utf8") };
  }

  const requests = (side) => {
    const f = join(side === "local" ? LOCAL_DIR : CLOUD_DIR, "requests.jsonl");
    return existsSync(f)
      ? readFileSync(f, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l))
      : [];
  };
  const turnRequestFor = (side, text) =>
    requests(side)
      .filter(
        (r) =>
          !r.turnId.startsWith("memory-l1-") &&
          r.round === 0 &&
          r.messages[r.messages.length - 1].content.includes(text),
      )
      .at(-1);

  // --- build and start two placements --------------------------------------
  const binDir = mkdtempSync(join(tmpdir(), "sumi-portmem-e2e-"));
  const bin = join(binDir, "state-dev");
  log("building state-dev…");
  const build = spawnSync("go", ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"], {
    cwd: API_DIR,
    stdio: "inherit",
  });
  check(build.status === 0, "state-dev builds");
  log("databases:", LOCAL_DB.replace(/\/\/[^@]*@/, "//***@"), CLOUD_DB.replace(/\/\/[^@]*@/, "//***@"));
  startService("local-state", LOCAL_DB, LOCAL_PORT, LOCAL_ADMIN);
  startService("cloud-state", CLOUD_DB, CLOUD_PORT, CLOUD_ADMIN);
  for (const base of [LOCAL, CLOUD]) {
    await waitFor(`${base} health`, async () => {
      try {
        return (await fetch(`${base}/health`)).ok;
      } catch {
        return false;
      }
    }, 30_000);
  }
  check(true, "local and cloud state services are up on separate databases");

  // --- 1. build a life with real memory on local ----------------------------
  const pid = uuidv7();
  const P = `/internal/core/personas/${pid}`;
  const created = await req(LOCAL, "POST", "/internal/core/personas", LOCAL_ADMIN, {
    persona_id: pid,
    display_name: "Portable memory secretary",
  });
  check(created.status === 201, "local persona created", created.text);
  const localToken = created.json.persona_token;
  const mem = async (base, token) => (await req(base, "GET", `${P}/memory`, token)).json;
  const outboxFor = async (base, token, inputId) =>
    (await req(base, "GET", `${P}/outbox?after_seq=0`, token)).json.outbox.filter(
      (o) => o.payload.input_id === inputId,
    );
  async function say(base, token, text) {
    const inputId = `in-${randomUUID()}`;
    const r = await req(base, "POST", `${P}/inputs`, token, {
      input_id: inputId,
      kind: "message",
      payload: { text },
      actor_kind: "human",
      actor_id: "owner",
      source_surface: "e2e-portable-memory",
    });
    check(r.status === 201, `input accepted: ${text.slice(0, 32)}`, r.text);
    await waitFor(
      `reply to "${text.slice(0, 28)}"`,
      async () => (await outboxFor(base, token, inputId)).length > 0 || (await outboxFor(base, token, inputId)),
    );
    return inputId;
  }

  const localCore = spawnChild("local", LOCAL, localToken, pid, true);
  // Five exchanges: each reply is ~11k estimated tokens, so every exchange's
  // records seal as one chunk when the next input_received boundary passes.
  await say(LOCAL, localToken, "COLOR=amber is my favorite color");
  await say(LOCAL, localToken, "TRAINS: the 08:12 is fastest");
  await say(LOCAL, localToken, "DINNER: Friday at the canal place");
  await say(LOCAL, localToken, "TRIP: weekend at the coast");
  await say(LOCAL, localToken, "CALL: ring the dentist tomorrow");
  await waitFor("chunk 1 preparing behind its gate", async () => {
    const m = await mem(LOCAL, localToken);
    return m.preparing === 1 || m;
  });
  check(true, "chunk 1 claimed for preparation on local");
  // Release chunk 1's branch; the candidate shelves, then applies once live
  // raw is over the limit, and the branch pipeline claims chunk 2 (gated).
  writeFileSync(join(LOCAL_DIR, "release-1"), "");
  await waitFor("chunk 1 applied and chunk 2 claimed", async () => {
    const m = await mem(LOCAL, localToken);
    return (m.applied === 1 && m.preparing === 1) || m;
  });
  const localShape = await mem(LOCAL, localToken);
  check(
    localShape.applied === 1 && localShape.preparing === 1 && localShape.sealed >= 2,
    "local memory: one applied fragment, one claim in flight, more sealed",
    localShape,
  );

  // --- 2. seal while chunk 2's preparation branch is still claimed ----------
  const cloudPlacement = await req(CLOUD, "GET", "/internal/core/placement", CLOUD_ADMIN);
  check(cloudPlacement.status === 200 && typeof cloudPlacement.json.placement_id === "string", "cloud exposes its placement id");
  const seal = await req(LOCAL, "POST", `${P}/transfers/${TRANSFER}/seal`, LOCAL_ADMIN, {
    destination_id: cloudPlacement.json.placement_id,
  });
  check(seal.status === 200 && seal.json.status === "sealed", "seal succeeds while a memory branch is in flight", seal.text);
  const cont = seal.json.continuity;
  check(
    cont.memory_applied === 1 && cont.memory_sealed === localShape.sealed + 1 &&
      (cont.memory_prepared ?? 0) === 0,
    "seal receipt carries the memory: the in-flight claim counts as sealed",
    cont,
  );
  const sealedShape = await mem(LOCAL, localToken);
  check(
    sealedShape.preparing === 0 && sealedShape.sealed === localShape.sealed + 1 &&
      sealedShape.applied === 1,
    "the fenced preparing claim returned to sealed on the source",
    { before: localShape, after: sealedShape },
  );
  const localExit = await Promise.race([
    localCore.exited,
    new Promise((r) => setTimeout(() => r(null), 25_000)),
  ]);
  check(localExit !== null, "the local core stopped after the seal", localCore.output().slice(-1500));

  // --- 3. bundle carries the chunk rows -------------------------------------
  const bundleRes = await req(LOCAL, "GET", `${P}/transfers/${TRANSFER}/bundle`, LOCAL_ADMIN);
  check(bundleRes.status === 200, "local bundle streamed", bundleRes.text.slice(0, 200));
  const bundle = bundleRes.text;
  const chunkRows = bundle
    .trimEnd()
    .split("\n")
    .filter((l) => l.includes(`"table":"core_memory_chunks"`))
    .map((l) => JSON.parse(l).data);
  check(chunkRows.length >= 3, "bundle carries the memory chunk rows", chunkRows);
  const c1 = chunkRows.find((c) => c.chunk_seq === 1);
  const c2 = chunkRows.find((c) => c.chunk_seq === 2);
  check(
    c1 && c1.status === "applied" && typeof c1.replacement === "string" &&
      c1.replacement.includes("L1 chunk 1:") && c1.applied_at !== null,
    "the applied fragment travels with its replacement text",
    c1,
  );
  check(
    c2 && c2.status === "sealed" && c2.claimed_generation === null && c2.claimed_at === null,
    "the in-flight claim crossed as sealed with no claim fields",
    c2,
  );
  check(
    chunkRows.every((c) => c.claimed_generation === null && c.claimed_at === null && c.status !== "preparing"),
    "no chunk carries a live execution claim",
    chunkRows.map((c) => ({ seq: c.chunk_seq, status: c.status })),
  );
  if (process.env.SUMI_PORTABLE_EVIDENCE_DIR) {
    writeFileSync(join(process.env.SUMI_PORTABLE_EVIDENCE_DIR, `bundle-${run}.ndjson`), bundle);
  }

  // A bundle carrying a live claim is refused at import — that path is
  // covered with a recomputed digest in TestImportRefusesMalformedMemoryChunks.
  const imported = await req(CLOUD, "POST", "/internal/core/transfers/import", CLOUD_ADMIN, bundle, true);
  check(imported.status === 201 && imported.json.status === "staged", "cloud staged the bundle", imported.text.slice(0, 300));
  const cloudPersona = await req(CLOUD, "POST", "/internal/core/personas", CLOUD_ADMIN, { persona_id: pid });
  check(cloudPersona.status === 200 && cloudPersona.json.persona.authority === "staged", "cloud persona is staged", cloudPersona.text);
  const cloudToken = cloudPersona.json.persona_token;

  const activated = await req(CLOUD, "POST", `${P}/transfers/${TRANSFER}/activate`, CLOUD_ADMIN);
  check(activated.status === 200, "cloud activated", activated.text);
  const completed = await req(LOCAL, "POST", `${P}/transfers/${TRANSFER}/complete`, LOCAL_ADMIN, {
    activate_proof: activated.json.activate_proof,
  });
  check(completed.status === 200 && completed.json.status === "completed", "local completed with the proof", completed.text);

  // --- 4. cloud continues the same memory -----------------------------------
  const cloudCore = spawnChild("cloud", CLOUD, cloudToken, pid, false);
  await say(CLOUD, cloudToken, "first message on cloud: what color do I like?");
  const first = turnRequestFor("cloud", "first message on cloud");
  check(
    first?.messages.some(
      (m) => m.content.startsWith("[Memory fragment") && m.content.includes("L1 chunk 1:"),
    ),
    "the first cloud turn already renders the carried applied fragment",
    first?.messages.map((m) => m.content.slice(0, 80)),
  );
  check(
    !first?.messages.some((m) => /^\[human\] COLOR=amber/.test(m.content)),
    "the applied range's raw originals stay behind the fragment",
    first?.messages.map((m) => m.content.slice(0, 60)),
  );
  check(
    requests("cloud").every((r) => r.turnId !== "memory-l1-1"),
    "cloud never re-prepares the settled applied chunk",
    requests("cloud").map((r) => r.turnId),
  );

  // The normalized chunk resumes as ordinary sealed work under cloud's writer.
  await waitFor("cloud claims the normalized chunk", async () => {
    return requests("cloud").some((r) => r.turnId === "memory-l1-2") || (await mem(CLOUD, cloudToken));
  });
  await waitFor("cloud memory shelf settles", async () => {
    const m = await mem(CLOUD, cloudToken);
    return (m.preparing === 0 && m.sealed === 0) || m;
  }, 60_000);
  const cloudShape = await mem(CLOUD, cloudToken);
  check(
    cloudShape.applied >= 1 && cloudShape.preparing === 0,
    "cloud continued the carried lifecycle: normalized chunk prepared under its writer",
    cloudShape,
  );

  // Carried chunk locators still resolve to the originals.
  await say(CLOUD, cloudToken, "HISTORY: open chunk_seq=1");
  const events = (await req(CLOUD, "GET", `${P}/events?after_seq=0&limit=1000`, cloudToken)).json.events;
  const historyResult = events
    .filter((e) => e.kind === "tool_result" && e.payload.tool === "conversation_history")
    .at(-1);
  check(
    historyResult && JSON.stringify(historyResult.payload.response ?? {}).includes("COLOR=amber"),
    "conversation_history opens the carried chunk's originals on cloud",
    historyResult?.payload,
  );

  // --- 5. local stays fenced and unchanged ----------------------------------
  const lateAcquire = await req(LOCAL, "POST", `${P}/writer/acquire`, localToken, {
    holder_id: "local-core-2",
    ttl_ms: 4000,
  });
  check(lateAcquire.status === 409, "no new local writer after the transfer", lateAcquire.text);
  const localAfter = await mem(LOCAL, localToken);
  check(
    localAfter.applied === sealedShape.applied && localAfter.sealed === sealedShape.sealed &&
      localAfter.preparing === 0,
    "the source's memory rows are unchanged after the destination ran",
    { sealed: sealedShape, after: localAfter },
  );

  const summary = {
    persona_id: pid,
    local_memory_at_seal: sealedShape,
    cloud_memory_final: cloudShape,
    carried_chunks: chunkRows.map((c) => ({
      chunk_seq: c.chunk_seq,
      status: c.status,
      first_seq: c.first_seq,
      last_seq: c.last_seq,
    })),
    cloud_branch_turns: requests("cloud").filter((r) => r.turnId.startsWith("memory-l1-")).map((r) => r.turnId),
  };
  writeFileSync(join(DIR, "summary.json"), JSON.stringify(summary, null, 2));
  log("summary", JSON.stringify(summary));
  log(`PASS — ${passed} checks: carried memory, fenced claims, resumption on real PG + real Go + real Node`);
  for (const c of children) c.kill("SIGTERM");
  process.exit(0);
}
