/**
 * Recovery-focused consumer tests against the REAL shared producer
 * (state-dev HTTP + real PG): periodic recovery under a live Supervisor,
 * outage-then-restore delivery, attention backlog progression, exact
 * immutable-payload replay after a lost reply, no-journal honesty, and
 * durable runner identity.
 *
 * Fault injection is a small togglable HTTP gate in front of the real
 * API — every request it forwards hits the real producer and real PG;
 * "down" answers 502 without forwarding (outage), "drop" forwards and
 * commits upstream then answers 502 (a lost reply after a landed write).
 * Labeled boundary: 'script' admission and jobfiles routes are not on
 * the producer branch, so script jobs are SQL-seeded and file-op
 * settlement is exercised only to the producer's route surface.
 *
 * Env:
 *   SUMI_TEST_DB_URL     postgres://... (dedicated throwaway DB — truncated)
 *   CONSUMER_API         http://host:port of an already-running producer
 *                        (else PRODUCER_STATE_DEV_BIN is spawned here)
 *   PRODUCER_STATE_DEV_BIN  state-dev binary built from the producer tree
 *   SQL_HELPER           "<bin>" invoked as `<bin> <SQL_DB_URL> <query>`
 *                        (pgx exec shim for containers without psql)
 *   SQL_DB_URL           DB URL as reachable from the helper (default DB_URL)
 *   PG_CONTAINER         docker container with psql (host-side alternative)
 *   PSQL_BIN             psql fallback
 *   WORKERD_BIN, RUNLIMITED_BIN — real worker for the live-job test
 */

import { test, before, after } from "node:test";
import * as assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as http from "node:http";
import * as crypto from "node:crypto";

import { StateClient, ConfiguredDiscovery } from "../src/api.ts";
import { defaultDispatcherPath } from "../src/runner.ts";
import { Reconciler } from "../src/reconcile.ts";
import { Journal } from "../src/journal.ts";
import { Supervisor, stableRunnerID } from "../src/main.ts";

const req = (k: string): string => {
  const v = process.env[k];
  if (!v) throw new Error(`${k} required`);
  return v;
};
const DB_URL = req("SUMI_TEST_DB_URL");
const STATE_DEV_BIN = process.env.PRODUCER_STATE_DEV_BIN ?? "";
const WORKERD_BIN = req("WORKERD_BIN");
const RUNLIMITED_BIN = req("RUNLIMITED_BIN");
const PSQL = process.env.PSQL_BIN ?? "psql";
const PG_CONTAINER = process.env.PG_CONTAINER ?? "";
const SQL_HELPER = process.env.SQL_HELPER ?? "";
const SQL_DB_URL = process.env.SQL_DB_URL ?? DB_URL;
const EXTERNAL_API = process.env.CONSUMER_API ?? "";

const ADMIN = "recovery-admin-token-0123456789abcdef";
const RUNTIME = "recovery-runtime-token-0123456789abcdef";
const RUNNER_ID = `recovery-it-${process.pid}`;
const REAL_API = EXTERNAL_API || "http://127.0.0.1:8185";

let apiProc: ReturnType<typeof spawn> | null = null;
let workDir = "";
let client: StateClient;
const apiLogs: string[] = [];

function uuidv7(): string {
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6]! & 0x0f) | 0x70;
  b[8] = (b[8]! & 0x3f) | 0x80;
  const h = [...b].map((x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

async function waitFor(fn: () => Promise<boolean> | boolean, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try { if (await fn()) return; } catch { /* retry */ }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

async function adminCall(method: string, path: string, body?: unknown): Promise<unknown> {
  const res = await fetch(`${REAL_API}${path}`, {
    method,
    headers: { "content-type": "application/json", authorization: `Bearer ${ADMIN}` },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const payload = await res.json().catch(() => null);
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${JSON.stringify(payload)}`);
  return payload;
}

async function makePersona(name: string): Promise<{ id: string; token: string }> {
  const id = uuidv7();
  const res = await adminCall("POST", "/internal/core/personas", { persona_id: id, display_name: name }) as { persona_token: string };
  return { id, token: res.persona_token };
}

function sql(q: string): void {
  if (SQL_HELPER) {
    execFileSync(SQL_HELPER, [SQL_DB_URL, q]);
  } else if (PG_CONTAINER) {
    const db = DB_URL.split("/").pop()!.split("?")[0]!;
    execFileSync("docker", ["exec", PG_CONTAINER, "psql", "-U", "postgres", "-d", db, "-Xq", "-c", q]);
  } else {
    execFileSync(PSQL, [DB_URL, "-Xq", "-c", q]);
  }
}

function seedJob(personaID: string, jobID: string, request: unknown, status = "queued", extra = ""): void {
  const r = JSON.stringify(request).replace(/'/g, "''");
  sql(`INSERT INTO core_jobs (persona_id, job_id, kind, request, status${extra ? ", claimed_by, claim_expires_at" : ""})` +
    ` VALUES ('${personaID}', '${jobID}', 'script', '${r}'::jsonb, '${status}'${extra})`);
}

function seedClaimed(personaID: string, jobID: string, status: string, runner = RUNNER_ID, expires = "now() + interval '1 hour'"): void {
  seedJob(personaID, jobID, { code: "x", input: null, limits: {} }, status,
    `, '${runner}', ${expires}`);
}

async function jobRow(personaID: string, jobID: string): Promise<Record<string, unknown>> {
  const { job } = await adminCall("GET", `/internal/core/personas/${personaID}/jobs/${jobID}`) as { job: Record<string, unknown> };
  return job;
}

/**
 * The fault-injection gate. rule(method, path) per request:
 *   "pass" — forward to the real producer, return its response.
 *   "down" — answer 502 without forwarding (API unreachable).
 *   "drop" — forward and let the upstream write COMMIT, then answer 502
 *            (the lost reply: producer has the effect, client saw failure).
 */
type GateAction = "pass" | "down" | "drop";
class Gate {
  url = "";
  rule: (method: string, path: string) => GateAction = () => "pass";
  private server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", async () => {
      const path = req.url ?? "/";
      const method = req.method ?? "GET";
      const action = this.rule(method, path);
      if (action === "down") {
        res.writeHead(502, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "gate: down" }));
        return;
      }
      try {
        const upstream = await fetch(`${REAL_API}${path}`, {
          method,
          headers: {
            "content-type": "application/json",
            authorization: String(req.headers.authorization ?? ""),
          },
          body: method === "GET" || method === "HEAD" ? undefined : Buffer.concat(chunks),
        });
        const body = Buffer.from(await upstream.arrayBuffer());
        if (action === "drop") {
          res.writeHead(502, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "gate: reply dropped after upstream commit" }));
          return;
        }
        res.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" });
        res.end(body);
      } catch {
        res.writeHead(502, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "gate: upstream error" }));
      }
    });
  });
  async start(): Promise<void> {
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    const addr = this.server.address() as { port: number };
    this.url = `http://127.0.0.1:${addr.port}`;
  }
  close(): void { this.server.close(); }
}

function newReconciler(wd = workDir, extra: Partial<{ attentionPage: number; attentionMaxPages: number }> = {}, api = REAL_API): Reconciler {
  return new Reconciler({
    client: new StateClient({ api, token: RUNTIME }),
    journal: new Journal(wd),
    runnerID: RUNNER_ID,
    log: () => {},
    ...extra,
  });
}

function plantExited(wd: string, personaID: string, jobID: string, wire: Record<string, unknown>, status = "done"): Journal {
  const journal = new Journal(wd);
  const j = journal.create({
    job_id: jobID, persona_id: personaID, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: "", stats_path: "", cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "0".repeat(64) }, usage_fact_id: `script:${jobID}:exec`,
  });
  journal.update(j, {
    status: "exited",
    result: { ...wire, terminal_status: status, error: status === "done" ? "" : "failed" },
    wire_result: wire,
  });
  return journal;
}

before(async () => {
  workDir = mkdtempSync(join(tmpdir(), "sumi-recovery-it-"));
  client = new StateClient({ api: REAL_API, token: RUNTIME });
  if (!EXTERNAL_API) {
    if (!STATE_DEV_BIN) throw new Error("PRODUCER_STATE_DEV_BIN required when CONSUMER_API is not set");
    apiProc = spawn(STATE_DEV_BIN, [], {
      env: {
        ...process.env,
        SUMI_DB_URL: DB_URL,
        SUMI_DB_CREATE: "1",
        SUMI_CORE_STATE_TOKEN: ADMIN,
        SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
        SUMI_STATE_LISTEN: "127.0.0.1:8185",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    apiProc.stdout?.on("data", (d) => apiLogs.push(String(d)));
    apiProc.stderr?.on("data", (d) => apiLogs.push(String(d)));
  }
  await waitFor(async () => {
    try {
      const res = await fetch(`${REAL_API}/internal/core/jobs/runnable?kinds=script`, { headers: { authorization: `Bearer ${RUNTIME}` } });
      return res.status === 200;
    } catch { return false; }
  }, 15000, "producer state-dev listen");
  sql("TRUNCATE core_jobs, core_personas CASCADE");
});

after(() => {
  apiProc?.kill();
  try { rmSync(workDir, { recursive: true, force: true }); } catch { /* best effort */ }
});

// ---------------------------------------------------------------- F379

test("recovery continues after an API outage — no supervisor restart needed", async () => {
  const p = await makePersona("outage");
  seedClaimed(p.id, "j-out", "running", RUNNER_ID);
  const gate = new Gate();
  gate.rule = () => "down";
  await gate.start();

  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-outage-"));
  plantExited(wd, p.id, "j-out", { value: 9, reason: "done", usage: { cpu_ms: 11 }, file_ops_pending: null });

  const sup = new Supervisor({
    api: gate.url, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 5000, heartbeatMs: 500, claimLimit: 4, cgroupMode: "prlimit",
    log: () => {},
    discovery: new ConfiguredDiscovery([]),
    maxConcurrent: 2, pollMs: 100, recoveryEveryMs: 200,
  });
  const running = sup.start();
  try {
    // Outage: nothing can be delivered, and nothing is falsely settled.
    await new Promise((r) => setTimeout(r, 600));
    const j1 = new Journal(wd).read("j-out")!;
    assert.notEqual(j1.status, "reported", "outage must not mark evidence delivered");
    let row = await jobRow(p.id, "j-out");
    assert.equal(row.status, "running", "row untouched during outage");

    // Restore the API WITHOUT a restart — the periodic pass must settle.
    gate.rule = () => "pass";
    await waitFor(async () => {
      const cur = new Journal(wd).read("j-out");
      return cur?.status === "reported";
    }, 15000, "periodic recovery delivers the owed result");
    row = await jobRow(p.id, "j-out");
    assert.equal(row.status, "done", "stored outcome delivered after restore");
    assert.equal((row.result as Record<string, unknown>).value, 9);
    const j2 = new Journal(wd).read("j-out")!;
    assert.equal(j2.usage_status, "recorded", "usage delivered too");
  } finally {
    sup.shutdown();
    gate.close();
    await running.catch(() => {});
  }
});

test("attention backlog deeper than one bounded pass progresses, unresolvable prefix cannot starve it", async () => {
  // The 'running' row is unresolvable (a missing journal is not proof
  // of exit — it defers until the lease sweep); the two 'lost' rows
  // must still resolve behind it, whichever cursor position each
  // holds. Page=1, one page per pass: only the persisted cursor lets
  // anything past the first row be reached at all.
  const p1 = await makePersona("backlog-stuck");
  const p2 = await makePersona("backlog-a");
  const p3 = await makePersona("backlog-b");
  seedClaimed(p1.id, "j-stuck", "running", RUNNER_ID);
  seedClaimed(p2.id, "j-b1", "lost", RUNNER_ID, "now() - interval '1 hour'");
  seedClaimed(p3.id, "j-b2", "lost", RUNNER_ID, "now() - interval '1 hour'");

  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-backlog-"));
  const rec = newReconciler(wd, { attentionPage: 1, attentionMaxPages: 1 });
  const outcome = async (pid: string, jid: string) =>
    ((await jobRow(pid, jid)).result as Record<string, unknown>)?.observed_outcome;

  const r1 = await rec.runRecovery(new Set());
  assert.equal(r1.attention.seen, 1, "bounded pass walks exactly one page");

  const r2 = await rec.runRecovery(new Set());
  assert.equal(r2.attention.seen, 1, "cursor continued, not restarted");
  // If the cursor had reset, pass 2 would re-see the stuck row and
  // resolve nothing — a resolution here proves it advanced.
  assert.ok((await outcome(p2.id, "j-b1")) || (await outcome(p3.id, "j-b2")),
    "pass 2 advanced past the stuck prefix and resolved a later row");

  const r3 = await rec.runRecovery(new Set());
  assert.equal(r3.attention.seen, 1);

  // Across the three passes all three rows were walked and the two
  // resolvable ones settled — the stuck prefix could not starve them.
  assert.ok(await outcome(p2.id, "j-b1"), "j-b1 resolved behind the unresolvable prefix");
  assert.ok(await outcome(p3.id, "j-b2"), "j-b2 resolved behind the unresolvable prefix");
  const stuck = await jobRow(p1.id, "j-stuck");
  assert.equal(stuck.status, "running", "unresolvable row deferred, not terminalized");
});

test("a concurrently running script survives a periodic recovery pass", async () => {
  const p = await makePersona("livejob");
  seedJob(p.id, "j-live", {
    code: 'export async function run(input){ await new Promise(r=>setTimeout(r,input.ms)); return "slept"; }',
    input: { ms: 2500 },
    limits: { cpu_seconds: 10, wall_ms: 30000, memory_mib: 64, output_bytes: 4096, log_bytes: 0, file_calls: 0, file_bytes: 0 },
  });
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-live-"));
  const sup = new Supervisor({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 15000, heartbeatMs: 300, claimLimit: 2, cgroupMode: "prlimit",
    log: () => {},
    discovery: new ConfiguredDiscovery([p.id]),
    maxConcurrent: 2, pollMs: 100, recoveryEveryMs: 250,
  });
  const running = sup.start();
  try {
    // Wait until the job is actually in flight under this supervisor.
    await waitFor(async () => {
      const j = new Journal(wd).read("j-live");
      return j?.status === "running" && j.pid != null;
    }, 15000, "live job dispatched");
    const inFlight = new Journal(wd).read("j-live")!;
    const pid = inFlight.pid!;
    // While it runs, plant an OWED exited journal + row for another job —
    // periodic recovery must settle it without touching the live one.
    const p2 = await makePersona("livejob-owed");
    seedClaimed(p2.id, "j-owed", "running", RUNNER_ID);
    plantExited(wd, p2.id, "j-owed", { value: 5, reason: "done", usage: { cpu_ms: 3 }, file_ops_pending: null });

    await waitFor(async () => (await jobRow(p.id, "j-live")).status === "done", 30000, "live job completes");
    const row = await jobRow(p.id, "j-live");
    assert.equal((row.result as Record<string, unknown>).value, "slept",
      "recovery passes did not kill or corrupt the live execution");
    // The planted debt settled while the live job was still owned.
    await waitFor(async () => (await jobRow(p2.id, "j-owed")).status === "done", 15000, "owed evidence settled concurrently");
    assert.equal(pid, inFlight.pid);
  } finally {
    sup.shutdown();
    await running.catch(() => {});
  }
});

// ---------------------------------------------------------------- F380

test("lost reply after a landed attach: identical replay lands; a divergent one would 409", async () => {
  const p = await makePersona("replay");
  seedClaimed(p.id, "j-rep", "lost", RUNNER_ID, "now() - interval '1 hour'");
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-replay-"));
  const wire = { reason: "supervisor_restarted", detail: "wait4 lost", usage: { cpu_ms: 5 }, file_ops_pending: null };
  plantExited(wd, p.id, "j-rep", wire, "failed");

  const gate = new Gate();
  let dropped = false;
  gate.rule = (_m, path) => {
    if (!dropped && path.includes("/lost-outcome")) {
      dropped = true;
      return "drop"; // commit upstream, lose the reply
    }
    return "pass";
  };
  await gate.start();

  const rec = newReconciler(wd, {}, gate.url);
  await rec.run();
  // The attach LANDED upstream but the client saw failure — journal must
  // stay unfinished, outcome still owed.
  const j1 = new Journal(wd).read("j-rep")!;
  assert.notEqual(j1.status, "reported", "lost reply keeps the attach eligible");
  let row = await jobRow(p.id, "j-rep");
  assert.ok((row.result as Record<string, unknown>)?.observed_outcome, "upstream commit did land");

  // Control: a DIFFERENT payload is refused — proving the retry must be
  // byte-identical, which is exactly what wire_result guarantees.
  const divergent = await client.attachLostOutcome(p.id, "j-rep", RUNNER_ID, {
    observed_status: "failed",
    result: { ...wire, file_ops_pending: 3 },
    error: "",
  });
  assert.equal(divergent.status, 409, "divergent replay is a real producer refusal");

  // Recovery retries with the stored wire payload — identical → replay 200.
  await rec.run();
  const j2 = new Journal(wd).read("j-rep")!;
  assert.equal(j2.status, "reported", "identical replay delivered");
  row = await jobRow(p.id, "j-rep");
  const out = (row.result as Record<string, unknown>).observed_outcome as Record<string, unknown>;
  assert.deepEqual(out.result, wire);
  gate.close();
});

test("getJob failure leaves the journal eligible; delivery lands on a later pass", async () => {
  const p = await makePersona("getjob");
  seedClaimed(p.id, "j-gj", "running", RUNNER_ID);
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-gj-"));
  plantExited(wd, p.id, "j-gj", { value: 2, reason: "done", usage: { cpu_ms: 4 }, file_ops_pending: null });

  const gate = new Gate();
  gate.rule = (m, path) =>
    m === "GET" && path.includes("/personas/") && /\/jobs\/[^/]+$/.test(path) ? "down" : "pass";
  await gate.start();

  const rec = newReconciler(wd, {}, gate.url);
  await rec.run();
  const j1 = new Journal(wd).read("j-gj")!;
  assert.equal(j1.status, "exited",
    "unreadable row must NOT mark the journal reported — the outcome is still owed");

  gate.rule = () => "pass";
  await rec.run();
  const j2 = new Journal(wd).read("j-gj")!;
  assert.equal(j2.status, "reported");
  const row = await jobRow(p.id, "j-gj");
  assert.equal(row.status, "done");
  assert.equal(j2.usage_status, "recorded");
  gate.close();
});

test("no-journal lost claim: durable record first, attach lands, usage failure recovered after restart", async () => {
  const p = await makePersona("njr");
  seedClaimed(p.id, "j-njr", "lost", RUNNER_ID, "now() - interval '1 hour'");
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-njr-"));

  const gate = new Gate();
  gate.rule = (_m, path) => (path.includes("/usage/record") ? "down" : "pass");
  await gate.start();

  // Pass 1: creates the durable journal, attaches the no-evidence
  // outcome — then usage delivery fails. The row leaves attention but
  // the journal keeps the obligation.
  const rec1 = newReconciler(wd, {}, gate.url);
  const r1 = await rec1.run();
  assert.ok(r1.attention.pending >= 1 || r1.attention.failed >= 1, "usage debt keeps the row pending");
  const j1 = new Journal(wd).read("j-njr")!;
  assert.ok(j1, "durable journal created before attempts");
  assert.equal(j1.status, "reported", "attach landed — reported is honest");
  assert.equal(j1.usage_status, "pending", "usage still owed and discoverable");
  let row = await jobRow(p.id, "j-njr");
  assert.ok((row.result as Record<string, unknown>)?.observed_outcome, "outcome attached");

  // "Restart": a NEW reconciler over the same journal dir — the durable
  // record is what rediscovers the usage obligation (the row is gone
  // from attention once the outcome attached).
  gate.rule = () => "pass";
  const rec2 = newReconciler(wd, {}, gate.url);
  await rec2.run();
  const j2 = new Journal(wd).read("j-njr")!;
  assert.equal(j2.usage_status, "unknown",
    "honest unknown usage delivered — no wait4 evidence existed to report");
  gate.close();
});

// ---------------------------------------------------------------- F381

test("runner identity: durable across restarts, refuses unrecoverable storage", async () => {
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-rid-"));
  const id1 = stableRunnerID(wd);
  assert.match(id1, /^scripts-.+-[0-9a-f]{16}$/, "random durable id, not pid-derived");
  // "Restart": same call must recover the persisted identity.
  assert.equal(stableRunnerID(wd), id1, "persisted identity survives restart");

  // An existing-but-empty runner-id is unrecoverable — refuse to claim
  // under an identity that would vanish next restart.
  const wd2 = mkdtempSync(join(tmpdir(), "sumi-rec-rid2-"));
  writeFileSync(join(wd2, "runner-id"), "");
  assert.throws(() => stableRunnerID(wd2), /empty|unrecoverable/i);

  // An unpersistable path (runner-id is a directory) must also refuse —
  // minting an ephemeral id here would orphan every claim at restart.
  const wd3 = mkdtempSync(join(tmpdir(), "sumi-rec-rid3-"));
  mkdirSync(join(wd3, "runner-id"));
  assert.throws(() => stableRunnerID(wd3), /unrecoverable|cannot persist|unreadable/i);

  // Explicit configuration always wins and is not re-validated.
  const prev = process.env.SUMI_RUNNER_ID;
  try {
    process.env.SUMI_RUNNER_ID = "explicit-runner";
    assert.equal(stableRunnerID(wd3), "explicit-runner");
  } finally {
    if (prev === undefined) delete process.env.SUMI_RUNNER_ID; else process.env.SUMI_RUNNER_ID = prev;
  }
});
