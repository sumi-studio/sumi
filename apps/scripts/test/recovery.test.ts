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
 *
 * On the merged producer every job state below is produced through the
 * real API surface — admission via POST /jobs, 'running' via the claim
 * route, 'lost' via a short lease + the claim-transaction sweep. No SQL
 * seeding: the producer's own transitions are the fixture. File effects
 * run through the real jobfiles routes against an in-process filesvc
 * protocol stub (keyed committed-receipt idempotency).
 *
 * Env:
 *   SUMI_TEST_DB_URL     postgres://... (a fresh <name>_<pid> database is
 *                        derived from it — per-run isolation, no truncation)
 *   CONSUMER_API         http://host:port of an already-running producer
 *                        (else PRODUCER_STATE_DEV_BIN is spawned here)
 *   PRODUCER_STATE_DEV_BIN  state-dev binary built from the producer tree
 *   WORKERD_BIN, RUNLIMITED_BIN — real worker for the live-job tests
 */

import { test, before, after } from "node:test";
import * as assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync, mkdirSync, writeFileSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as http from "node:http";
import * as crypto from "node:crypto";

import { StateClient, ConfiguredDiscovery, SharedDiscovery } from "../src/api.ts";
import { defaultDispatcherPath, Runner } from "../src/runner.ts";
import { Reconciler } from "../src/reconcile.ts";
import { Journal } from "../src/journal.ts";
import { Supervisor, stableRunnerID } from "../src/main.ts";
import { childrenOf } from "../src/proc.ts";
import { StubFileSvc } from "./filesvc_stub.mts";

const req = (k: string): string => {
  const v = process.env[k];
  if (!v) throw new Error(`${k} required`);
  return v;
};
// Fresh database + runner identity per run — random, not pid-derived:
// container pids are constant across runs, so pid-based names get reused
// and stale rows pollute claim/attention/discovery assertions.
const RUN_SUFFIX = crypto.randomUUID().slice(0, 8);
const DB_URL = req("SUMI_TEST_DB_URL").replace(/\/[^/?]+(\?.*)?$/, `/sumi_recovery_${RUN_SUFFIX}$1`);
const STATE_DEV_BIN = process.env.PRODUCER_STATE_DEV_BIN ?? "";
const WORKERD_BIN = req("WORKERD_BIN");
const RUNLIMITED_BIN = req("RUNLIMITED_BIN");
const EXTERNAL_API = process.env.CONSUMER_API ?? "";

const ADMIN = "recovery-admin-token-0123456789abcdef";
const RUNTIME = "recovery-runtime-token-0123456789abcdef";
const RUNNER_ID = `recovery-it-${RUN_SUFFIX}`;
const REAL_API = EXTERNAL_API || "http://127.0.0.1:8185";

let apiProc: ReturnType<typeof spawn> | null = null;
let workDir = "";
let client: StateClient;
const stub = new StubFileSvc();
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

/** True when a pid is gone OR a zombie: kill(pid,0) reports a zombie
 *  as "alive", but a zombie can never execute again — for the
 *  child-dead assertions it is dead. */
function notRunning(pid: number): boolean {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    const state = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0];
    return state === "Z" || state === "X";
  } catch {
    return true;
  }
}

/** Every LIVE workerd on the box — a leaked orphan reparents to init
 *  and escapes any subtree walk; only a comm scan sees it. Zombies
 *  (state Z/X) are excluded: they are dead, awaiting a reap that the
 *  container's pid-1 node may never perform. */
function workerdPids(): number[] {
  const out: number[] = [];
  for (const name of readdirSync("/proc")) {
    if (!/^\d+$/.test(name)) continue;
    try {
      if (readFileSync(`/proc/${name}/comm`, "utf8").trim() !== "workerd") continue;
      const stat = readFileSync(`/proc/${name}/stat`, "utf8");
      const state = stat.slice(stat.lastIndexOf(")") + 2).split(" ")[0];
      if (state !== "Z" && state !== "X") out.push(Number(name));
    } catch { /* gone */ }
  }
  return out;
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

// Job states are produced through the producer's own transitions, never
// SQL: admission via POST /jobs, a live 'running' claim via the claim
// route, and 'lost' via a short lease plus the claim-transaction sweep.
async function submitJob(personaID: string, jobID: string, request: unknown): Promise<void> {
  await adminCall("POST", `/internal/core/personas/${personaID}/jobs`, {
    job_id: jobID, kind: "script", request,
  });
}

/** Real admission + real claim: the row is 'running', claimed by RUNNER_ID. */
async function claimRunning(personaID: string, jobID: string, request: unknown = { code: "x" }, leaseMs = 3_600_000): Promise<void> {
  await submitJob(personaID, jobID, request);
  const { claimed } = await client.claimJobs(personaID, RUNNER_ID, leaseMs, 4);
  if (!claimed.some((j) => j.job_id === jobID)) throw new Error(`${jobID} was not claimed`);
}

/** Real admission + real claim + real lease expiry + real sweep: 'lost'. */
async function claimToLost(personaID: string, jobID: string): Promise<void> {
  await submitJob(personaID, jobID, { code: "x" });
  await client.claimJobs(personaID, RUNNER_ID, 400, 4);
  // Each later claim pass runs the expiry sweep inside its tx; retry
  // until the lease has demonstrably expired rather than trusting one
  // fixed sleep — HTTP latency inside a busy fixture can compress a
  // single wait below the lease.
  const deadline = Date.now() + 10_000;
  for (;;) {
    await new Promise((r) => setTimeout(r, 500));
    await client.claimJobs(personaID, RUNNER_ID, 400, 4);
    const row = await jobRow(personaID, jobID);
    if (row.status === "lost") return;
    if (Date.now() > deadline) throw new Error(`${jobID} not swept to lost: ${row.status}`);
  }
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
  /** Optional upstream delay applied to paths containing delayMatch —
   *  simulates a slow API (stalled cancel/claim responses) while every
   *  forwarded request still commits on the real producer. */
  delayMs = 0;
  delayMatch = "";
  /** Observed request paths — lets tests wait for a specific call to
   *  be in flight before injecting a fault (e.g. SIGTERM mid-claim). */
  onRequest: ((method: string, path: string) => void) | null = null;
  private server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", async () => {
      const path = req.url ?? "/";
      const method = req.method ?? "GET";
      try { this.onRequest?.(method, path); } catch { /* observer only */ }
      const action = this.rule(method, path);
      if (action === "down") {
        res.writeHead(502, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "gate: down" }));
        return;
      }
      try {
        if (this.delayMs > 0 && path.includes(this.delayMatch)) {
          await new Promise((r) => setTimeout(r, this.delayMs));
        }
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
  const stubURL = await stub.start();
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
        SUMI_FILESVC_URL: stubURL,
        SUMI_FILESVC_TOKEN: "svc-token",
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
});

after(async () => {
  apiProc?.kill();
  await stub.stop();
  try { rmSync(workDir, { recursive: true, force: true }); } catch { /* best effort */ }
});

// ---------------------------------------------------------------- F379

test("recovery continues after an API outage — no supervisor restart needed", async () => {
  const p = await makePersona("outage");
  await claimRunning(p.id, "j-out");
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
  await claimRunning(p1.id, "j-stuck");
  await claimToLost(p2.id, "j-b1");
  await claimToLost(p3.id, "j-b2");

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
  await submitJob(p.id, "j-live", {
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
    await claimRunning(p2.id, "j-owed");
    plantExited(wd, p2.id, "j-owed", { value: 5, reason: "done", usage: { cpu_ms: 3 }, file_ops_pending: null });

    await waitFor(async () => (await jobRow(p.id, "j-live")).status === "done", 30000, "live job completes");
    const row = await jobRow(p.id, "j-live");
    assert.equal((row.result as Record<string, unknown>).value, "slept",
      "recovery passes did not kill or corrupt the live execution");
    // The planted debt settled while the live job was still owned.
    await waitFor(async () => (await jobRow(p2.id, "j-owed")).status === "done", 15000, "owed evidence settled concurrently");
    assert.equal(pid, inFlight.pid);
  } finally {
    await sup.shutdown();
    await running.catch(() => {});
  }
});

// ---------------------------------------------------------------- F380

test("lost reply after a landed attach: identical replay lands; a divergent one would 409", async () => {
  const p = await makePersona("replay");
  await claimToLost(p.id, "j-rep");
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
  await claimRunning(p.id, "j-gj");
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
  await claimToLost(p.id, "j-njr");
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

test("file effects: terminal+usage delivered while one effect is unresolved stays retryable until the ledger settles", { timeout: 60_000 }, async () => {
  const p = await makePersona("fop");
  // Real admission and a real durable claim.
  await submitJob(p.id, "j-fx", {
    code: 'export async function run(){ return "fx-done" }',
  });
  const { claimed } = await client.claimJobs(p.id, RUNNER_ID, 30_000, 1);
  const job = claimed.find((j) => j.job_id === "j-fx")!;

  // While the claim is live, admit a mutating op whose upstream response
  // is destroyed after the commit — the durable ledger records 'unknown'.
  stub.faults.dropResponse = true;
  const wr = await fetch(`${REAL_API}/internal/core/personas/${p.id}/jobs/j-fx/files/write`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${RUNTIME}` },
    body: JSON.stringify({ runner_id: RUNNER_ID, path: "fx.txt", data_base64: Buffer.from("x").toString("base64") }),
  });
  stub.faults.dropResponse = false;
  const wrBody = await wr.json() as { op: { status: string } };
  assert.equal(wrBody.op.status, "unknown", "upstream uncertainty journaled, not invented");

  // The runner finishes while resolve attempts cannot reach the API:
  // terminal result + measured usage land, one effect stays pending.
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-fop-"));
  const gate = new Gate();
  gate.rule = (m, path) => (m === "POST" && path.includes("/files/ops/") && path.endsWith("/resolve") ? "down" : "pass");
  await gate.start();
  const runner = new Runner({
    api: gate.url, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 1, cgroupMode: "prlimit", log: () => {},
  });
  await runner.runJob(job);
  const row1 = await jobRow(p.id, "j-fx");
  assert.equal(row1.status, "done", "terminal result delivered despite unresolved effect");
  const j1 = new Journal(wd).read("j-fx")!;
  assert.equal(j1.status, "reported");
  assert.equal(j1.usage_status, "recorded");
  assert.equal(j1.file_ops_pending, 1, "the ledger's real count is journaled");
  // THE F380 invariant: a delivered job with an unresolved effect is
  // still recoverable — under the old selection it was invisible forever.
  assert.ok(new Journal(wd).unfinished().some((j) => j.job_id === "j-fx"),
    "unresolved effect keeps the journal eligible for retry");

  // Recovery path restores: a later pass resolves the op by keyed resend —
  // no script re-execution, no restart; the receipt proves single commit.
  gate.rule = () => "pass";
  await newReconciler(wd, {}, gate.url).runRecovery(new Set());
  const j2 = new Journal(wd).read("j-fx")!;
  assert.equal(j2.file_ops_pending, 0);
  assert.ok(!new Journal(wd).unfinished().some((j) => j.job_id === "j-fx"), "all obligations settled");
  const { ops, pending } = await client.listFileOps(p.id, "j-fx");
  assert.equal(pending, 0);
  assert.equal(ops.length, 1, "resend is the same durable op, never a fresh one");
  assert.equal(ops[0]!.status, "settled");
  const row2 = await jobRow(p.id, "j-fx");
  assert.equal(row2.status, "done", "no re-execution — verdict and result untouched");
  assert.equal((row2.result as Record<string, unknown>).value, "fx-done");
  gate.close();
});

test("claims are bounded by free capacity — a backlog never strands owned claims", { timeout: 90_000 }, async () => {
  const p = await makePersona("backlog-run");
  // Four queued jobs discovered dynamically under a supervisor that can
  // run two at once but would claim four per pass if unbounded (F382:
  // claiming beyond free capacity left owned/running jobs with no
  // journal until their leases expired to 'lost').
  for (const id of ["j-c1", "j-c2", "j-c3", "j-c4"]) {
    await submitJob(p.id, id, {
      code: 'export async function run(){ return "ok" }',
      limits: { cpu_seconds: 5, wall_ms: 20000, memory_mib: 64 },
    });
  }
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-cap-"));
  const sup = new Supervisor({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 10_000, heartbeatMs: 300, claimLimit: 4, cgroupMode: "prlimit",
    log: () => {},
    discovery: new SharedDiscovery(new StateClient({ api: REAL_API, token: RUNTIME }), ["script"], 8),
    maxConcurrent: 2, pollMs: 200, recoveryEveryMs: 60_000,
  });
  const running = sup.start();
  try {
    for (const id of ["j-c1", "j-c2", "j-c3", "j-c4"]) {
      await waitFor(async () => (await jobRow(p.id, id)).status === "done", 60_000, `${id} executed`);
    }
    for (const id of ["j-c1", "j-c2", "j-c3", "j-c4"]) {
      const row = await jobRow(p.id, id);
      assert.equal(row.status, "done", "every admitted job ran exactly once");
      const j = new Journal(wd).read(id);
      assert.ok(j, `${id} has an execution journal — no owned claim left unjournaled`);
      assert.equal(j!.status, "reported");
    }
  } finally {
    await sup.shutdown();
    await running.catch(() => {});
  }
});


test("lost row + evidence-less spawned journal resolves with the honest indeterminate outcome (F389)", { timeout: 30_000 }, async () => {
  const p = await makePersona("wedge");
  await claimToLost(p.id, "j-wedge"); // real API: claim -> lease expiry -> sweep to lost
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-wedge-"));
  const journal = new Journal(wd);
  // The crash-window journal shape: created between journal.create and
  // the pid update, or left by a spawn-time fault — no result, no pid,
  // nothing to signal.
  journal.create({
    job_id: "j-wedge", persona_id: p.id, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: "", stats_path: "", cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "0".repeat(64) },
    usage_fact_id: "script:j-wedge:exec",
  });
  const res = await newReconciler(wd).run();
  const row = await jobRow(p.id, "j-wedge");
  assert.equal(row.status, "lost", "immutable verdict untouched");
  const outcome = (row.result as Record<string, unknown>)?.observed_outcome as Record<string, unknown> | undefined;
  assert.ok(outcome, "honest indeterminate outcome attached — the row must not wedge in attention");
  assert.equal((outcome.result as Record<string, unknown>).reason, "runner_restart_no_evidence");
  const j = journal.read("j-wedge")!;
  assert.equal(j.status, "reported", "journal settles after the attach lands");
  assert.ok(res.attention.resolved >= 1, "attention pass resolved the row");
  const att = await client.attentionJobs(RUNNER_ID, ["script"], 64);
  assert.ok(!att.jobs.some((r) => r.job_id === "j-wedge"), "row left the attention set");
});

test("a single job's preparation failure is contained — sibling completes and the loop keeps claiming (F388)", { timeout: 90_000 }, async () => {
  const p = await makePersona("contain");
  await submitJob(p.id, "j-bad", { code: 'export async function run(){ return "never runs" }' });
  await submitJob(p.id, "j-good", { code: 'export async function run(){ return "good-ok" }' });
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-contain-"));
  // Occupy the per-job dir as a regular FILE — mkdirSync(run-j-bad)
  // throws EEXIST inside runJob's preparation section: the fault that
  // used to escape as an unhandledRejection and kill the whole
  // supervisor process.
  mkdirSync(join(wd, "tmp"), { recursive: true });
  writeFileSync(join(wd, "tmp", "run-j-bad"), "occupied");
  const sup = new Supervisor({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 4, cgroupMode: "prlimit",
    log: () => {},
    discovery: new SharedDiscovery(new StateClient({ api: REAL_API, token: RUNTIME }), ["script"], 8),
    maxConcurrent: 2, pollMs: 200, recoveryEveryMs: 60_000,
  });
  const running = sup.start();
  try {
    await waitFor(async () => (await jobRow(p.id, "j-bad")).status === "failed", 30_000, "j-bad reported failed honestly");
    const bad = await jobRow(p.id, "j-bad");
    assert.equal((bad.result as Record<string, unknown>).reason, "runner_error",
      "the job reports an honest preparation failure — not a process crash, not a stranded claim");
    await waitFor(async () => (await jobRow(p.id, "j-good")).status === "done", 30_000, "sibling unaffected");
    // The loop itself survived: work submitted afterwards is claimed
    // and executed normally.
    await submitJob(p.id, "j-after", { code: 'export async function run(){ return "after-ok" }' });
    await waitFor(async () => (await jobRow(p.id, "j-after")).status === "done", 30_000, "supervisor still claiming after the failure");
  } finally {
    await sup.shutdown();
    await running.catch(() => {});
  }
});

test("config.capnp credential file is removed once the worker binds; stale copies swept at startup (F390)", { timeout: 60_000 }, async () => {
  const p = await makePersona("cfg-clean");
  await submitJob(p.id, "j-cfg", { code: 'export async function run(){ return "cfg-ok" }' });
  const { claimed } = await client.claimJobs(p.id, RUNNER_ID, 30_000, 1);
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-cfg-"));
  const runner = new Runner({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 1, cgroupMode: "prlimit", log: () => {},
  });
  await runner.runJob(claimed[0]!);
  assert.equal((await jobRow(p.id, "j-cfg")).status, "done");
  assert.ok(!existsSync(join(wd, "tmp", "run-j-cfg", "config.capnp")),
    "the runtime-token copy is removed once the worker has read it");

  // Residue a dead run left behind is swept at startup — journals and
  // stats stay, only the credential file is removed.
  mkdirSync(join(wd, "tmp", "run-j-stale"), { recursive: true });
  writeFileSync(join(wd, "tmp", "run-j-stale", "config.capnp"), "residue");
  assert.equal(runner.cleanStaleCredentialFiles(), 1);
  assert.ok(!existsSync(join(wd, "tmp", "run-j-stale", "config.capnp")));
});

test("shipped entrypoint: boots on the env contract, executes a job, and SIGTERM cancels in-flight work gracefully (F387)", { timeout: 90_000 }, async () => {
  const p = await makePersona("entry");
  const wd = mkdtempSync(join(tmpdir(), "sumi-entry-"));
  const runBin = new URL("../run.mjs", import.meta.url).pathname;

  // Fatal startup: missing required env exits non-zero and names it —
  // the process never half-starts under a broken contract.
  const badEnv: NodeJS.ProcessEnv = { ...process.env, SUMI_WORK_DIR: wd };
  delete badEnv["SUMI_STATE_API"]; delete badEnv["SUMI_STATE_TOKEN"]; delete badEnv["SUMI_WORKERD_BIN"];
  const miss = spawn(process.execPath, [runBin], { env: badEnv });
  const missErr: string[] = [];
  miss.stderr?.on("data", (d) => missErr.push(String(d)));
  const missCode = await new Promise<number>((r) => miss.on("exit", (c) => r(c ?? -1)));
  assert.notEqual(missCode, 0, "missing env must fail startup honestly");

  // Real lifecycle through the shipped command: a fast job completes;
  // a sleeping job is in flight when SIGTERM arrives and must end
  // 'cancelled' — the honest cancel path, not orphaned-to-lost.
  await submitJob(p.id, "j-fast", { code: 'export async function run(){ return "entry-ok" }' });
  await submitJob(p.id, "j-slow", {
    code: 'export async function run(){ await new Promise(r=>setTimeout(r,60000)); return "never" }',
    limits: { wall_ms: 120000, cpu_seconds: 120 },
  });
  const proc = spawn(process.execPath, [runBin], {
    env: {
      ...process.env,
      SUMI_STATE_API: REAL_API, SUMI_STATE_TOKEN: RUNTIME,
      SUMI_WORKERD_BIN: WORKERD_BIN, SUMI_RUNLIMITED_BIN: RUNLIMITED_BIN,
      SUMI_CGROUP_MODE: "prlimit", SUMI_WORK_DIR: wd,
      SUMI_POLL_MS: "200", SUMI_HEARTBEAT_MS: "300",
      SUMI_MAX_CONCURRENT: "2", SUMI_CLAIM_LIMIT: "4",
      SUMI_SHUTDOWN_GRACE_MS: "15000", SUMI_LEASE_MS: "30000",
    },
  });
  const out: string[] = [];
  proc.stdout?.on("data", (d) => out.push(String(d)));
  proc.stderr?.on("data", (d) => out.push(String(d)));
  try {
    await waitFor(async () => (await jobRow(p.id, "j-fast")).status === "done", 45_000,
      "entrypoint discovered, claimed and executed j-fast");
    await waitFor(async () => (await jobRow(p.id, "j-slow")).status === "running", 15_000, "j-slow in flight");
    proc.kill("SIGTERM");
    const code = await new Promise<number>((r) => proc.on("exit", (c) => r(c ?? -1)));
    assert.equal(code, 0, `graceful shutdown must exit 0; logs: ${out.join("")}`);
    const slow = await jobRow(p.id, "j-slow");
    assert.equal(slow.status, "cancelled", "in-flight work ends by honest cancel, not orphaned-lost");
  } finally {
    proc.kill("SIGKILL");
  }
});

test("waitReady failure removes the launch credential — config.capnp gone on a failed launch (F390)", { timeout: 60_000 }, async () => {
  const p = await makePersona("ready-fail");
  await submitJob(p.id, "j-nr", { code: 'export async function run(){ return "never" }' });
  const { claimed } = await client.claimJobs(p.id, RUNNER_ID, 30_000, 1);
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-nr-"));
  // /bin/false exits immediately: spawn succeeds, waitReady observes the
  // early exit — the inner-catch path that used to bypass config cleanup.
  const runner = new Runner({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: "/bin/false", runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 1, cgroupMode: "prlimit", log: () => {},
  });
  await runner.runJob(claimed[0]!);
  const row = await jobRow(p.id, "j-nr");
  assert.equal(row.status, "failed", "failed launch is reported honestly");
  assert.equal((row.result as Record<string, unknown>).reason, "runner_error");
  assert.ok(!existsSync(join(wd, "tmp", "run-j-nr", "config.capnp")),
    "the token-bearing config is removed even when the launch never bound");
});

test("post-spawn/pre-pid-persist fault is contained: child killed, config removed, honest failed (F390/F388)", { timeout: 60_000 }, async () => {
  const p = await makePersona("pid-fail");
  await submitJob(p.id, "j-pp", { code: 'export async function run(){ await new Promise(r=>setTimeout(r,30000)); return "x" }' });
  const { claimed } = await client.claimJobs(p.id, RUNNER_ID, 30_000, 1);
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-pp-"));
  const runner = new Runner({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 1, cgroupMode: "prlimit", log: () => {},
  });
  // Inject exactly the crash window: the journal write that persists the
  // spawned pid throws — a child now exists that no journal names.
  const orig = runner.journal.update.bind(runner.journal);
  let injected = false;
  runner.journal.update = ((jj: never, fields: Record<string, unknown>) => {
    if (!injected && fields["pid"] != null) {
      injected = true;
      throw new Error("injected pid-persist fault");
    }
    return orig(jj, fields as never);
  }) as typeof runner.journal.update;
  await runner.runJob(claimed[0]!);
  const row = await jobRow(p.id, "j-pp");
  assert.equal(row.status, "failed", "the spawn-window fault reports honest failed, not a crash");
  assert.equal((row.result as Record<string, unknown>).reason, "runner_error");
  // The spawned child was killed via the held handle — nothing leaked,
  // including an orphan reparented to init that a subtree walk misses.
  await waitFor(() => workerdPids().length === 0, 5_000, "no workerd survives anywhere");
  assert.ok(!existsSync(join(wd, "tmp", "run-j-pp", "config.capnp")),
    "launch credential removed on the post-spawn fault path too");
});

test("shutdown is truly bounded under stalled cancels: deadline kills children, reports land (F391)", { timeout: 60_000 }, async () => {
  const p = await makePersona("stall");
  for (const id of ["j-s1", "j-s2"]) {
    await submitJob(p.id, id, {
      code: 'export async function run(){ await new Promise(r=>setTimeout(r,60000)); return "x" }',
      limits: { wall_ms: 120000, cpu_seconds: 120 },
    });
  }
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-stall-"));
  const sup = new Supervisor({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 4, cgroupMode: "prlimit",
    log: () => {},
    discovery: new SharedDiscovery(new StateClient({ api: REAL_API, token: RUNTIME }), ["script"], 8),
    maxConcurrent: 2, pollMs: 200, recoveryEveryMs: 60_000,
    shutdownSettleMs: 800,
  });
  const running = sup.start();
  try {
    for (const id of ["j-s1", "j-s2"]) {
      await waitFor(async () => (await jobRow(p.id, id)).status === "running", 30_000, `${id} running`);
    }
    // Every cancel stalls forever — the OLD code would serialize these
    // 15s-timeout calls before the grace clock even started.
    const clientRef = sup.runner.client as unknown as { cancelJob: (p: string, j: string) => Promise<unknown> };
    const origCancel = clientRef.cancelJob;
    clientRef.cancelJob = () => new Promise<unknown>(() => {});
    const t0 = Date.now();
    await sup.shutdown(300);
    const elapsed = Date.now() - t0;
    clientRef.cancelJob = origCancel;
    assert.ok(elapsed < 5_000, `shutdown returned in ${elapsed}ms — stalled cancels must not multiply the bound`);
    // Deadline kills: both workerd children are dead — nothing was left
    // to "its own rlimits" (an idle sleeper would outlive us forever).
    for (const id of ["j-s1", "j-s2"]) {
      const j = new Journal(wd).read(id)!;
      assert.ok(j.pid == null || notRunning(j.pid), `${id} child pid=${j.pid} is dead after the deadline kill`);
      await waitFor(async () => {
        const row = await jobRow(p.id, id);
        return row.status === "failed" || row.status === "cancelled";
      }, 15_000, `${id} reaches an honest terminal verdict`);
      const row = await jobRow(p.id, id);
      assert.equal(row.status, "failed", "killed-at-deadline reports failed with real evidence");
      assert.equal((row.result as Record<string, unknown>).reason, "worker_error");
    }
  } finally {
    await sup.shutdown();
    await running.catch(() => {});
  }
});

test("entrypoint: SIGTERM during an in-flight claim — the raced claim is accounted, never stranded (F391)", { timeout: 60_000 }, async () => {
  const p = await makePersona("claim-race");
  await submitJob(p.id, "j-race", { code: 'export async function run(){ return 1 }' });
  const gate = new Gate();
  await gate.start();
  gate.delayMatch = "/claim"; // every claim response is 1.5s late — the
  gate.delayMs = 1500;        // signal lands while the claim is in flight
  let claimsSeen = 0;
  gate.onRequest = (method, path) => { if (method === "POST" && path.includes("/claim")) claimsSeen++; };
  const wd = mkdtempSync(join(tmpdir(), "sumi-entry-race-"));
  const runBin = new URL("../run.mjs", import.meta.url).pathname;
  const proc = spawn(process.execPath, [runBin], {
    env: {
      ...process.env,
      SUMI_STATE_API: gate.url, SUMI_STATE_TOKEN: RUNTIME,
      SUMI_WORKERD_BIN: WORKERD_BIN, SUMI_RUNLIMITED_BIN: RUNLIMITED_BIN,
      SUMI_CGROUP_MODE: "prlimit", SUMI_WORK_DIR: wd, SUMI_RUNNER_ID: RUNNER_ID,
      SUMI_POLL_MS: "200", SUMI_HEARTBEAT_MS: "300",
      SUMI_MAX_CONCURRENT: "2", SUMI_CLAIM_LIMIT: "4",
      SUMI_SHUTDOWN_GRACE_MS: "3000", SUMI_SHUTDOWN_SETTLE_MS: "1000",
      SUMI_LEASE_MS: "30000",
    },
  });
  const out: string[] = [];
  proc.stdout?.on("data", (d) => out.push(String(d)));
  proc.stderr?.on("data", (d) => out.push(String(d)));
  const t0 = Date.now();
  try {
    // Wait until the claim request is actually in flight, then signal —
    // the delayed response lands after stop, exercising the race.
    await waitFor(() => claimsSeen >= 1, 15_000, "claim request in flight");
    proc.kill("SIGTERM");
    const code = await new Promise<number>((r) => proc.on("exit", (c) => r(c ?? -1)));
    const elapsed = Date.now() - t0;
    assert.equal(code, 0, `bounded shutdown exits 0; logs: ${out.join("")}`);
    assert.ok(elapsed < 10_000, `process exited in ${elapsed}ms`);
    // The claim resolved post-stop: the durable reservation is reported
    // 'cancelled'/shutdown_before_start — provably never executed — not
    // stranded 'running' until lease expiry.
    await waitFor(async () => (await jobRow(p.id, "j-race")).status === "cancelled", 15_000,
      "raced claim reaches cancelled");
    const row = await jobRow(p.id, "j-race");
    assert.equal((row.result as Record<string, unknown>).reason, "shutdown_before_start",
      "honest verdict: claimed during shutdown, provably never executed");
    assert.equal(workerdPids().length, 0, "no workerd ever spawned");
  } finally {
    proc.kill("SIGKILL");
    gate.close();
  }
});

test("entrypoint: short-grace SIGTERM kills the real child; restart reconcile recovers evidence (F391)", { timeout: 90_000 }, async () => {
  const p = await makePersona("grace-kill");
  await submitJob(p.id, "j-gk", {
    code: 'export async function run(){ await new Promise(r=>setTimeout(r,60000)); return "never" }',
    limits: { wall_ms: 120000, cpu_seconds: 120 },
  });
  const wd = mkdtempSync(join(tmpdir(), "sumi-entry-gk-"));
  const runBin = new URL("../run.mjs", import.meta.url).pathname;
  const env = {
    ...process.env,
    SUMI_STATE_API: REAL_API, SUMI_STATE_TOKEN: RUNTIME,
    SUMI_WORKERD_BIN: WORKERD_BIN, SUMI_RUNLIMITED_BIN: RUNLIMITED_BIN,
    SUMI_CGROUP_MODE: "prlimit", SUMI_WORK_DIR: wd, SUMI_RUNNER_ID: RUNNER_ID,
    SUMI_POLL_MS: "200", SUMI_HEARTBEAT_MS: "300",
    SUMI_MAX_CONCURRENT: "2", SUMI_CLAIM_LIMIT: "4",
    SUMI_SHUTDOWN_GRACE_MS: "100", SUMI_SHUTDOWN_SETTLE_MS: "800",
    SUMI_LEASE_MS: "5000",
  };
  const proc = spawn(process.execPath, [runBin], { env });
  const out: string[] = [];
  proc.stdout?.on("data", (d) => out.push(String(d)));
  proc.stderr?.on("data", (d) => out.push(String(d)));
  try {
    await waitFor(async () => {
      const j = new Journal(wd).read("j-gk");
      return j != null && j.pid != null && (await jobRow(p.id, "j-gk")).status === "running";
    }, 30_000, "j-gk claimed and running");
    const pid = new Journal(wd).read("j-gk")!.pid!;
    const t0 = Date.now();
    proc.kill("SIGTERM");
    const code = await new Promise<number>((r) => proc.on("exit", (c) => r(c ?? -1)));
    const elapsed = Date.now() - t0;
    assert.equal(code, 0, `bounded shutdown exits 0; logs: ${out.join("")}`);
    assert.ok(elapsed < 8_000, `process exited in ${elapsed}ms — grace+settle+kill, not an open drain`);
    // THE F391 invariant: the child does not outlive the bound — wall_ms
    // lives in the dead supervisor's drive loop; nothing else bound it.
    assert.ok(notRunning(pid), `workerd pid=${pid} dead after deadline kill`);
    // Honest aftermath: the drive loop's kill-observation report lands
    // ('failed'), or the row expires to 'lost' and a restart reconcile
    // attaches the indeterminate outcome — never a silent orphan.
    let row = await jobRow(p.id, "j-gk");
    if (row.status === "running" || row.status === "cancel_requested") {
      // Report didn't land inside the settle window — restart the real
      // entrypoint: startup reconcile + claim sweep resolves the row.
      const proc2 = spawn(process.execPath, [runBin], { env });
      try {
        await waitFor(async () => {
          const r2 = await jobRow(p.id, "j-gk");
          return r2.status === "failed" || r2.status === "cancelled" || r2.status === "lost";
        }, 30_000, "restart reconcile resolves the row");
      } finally {
        proc2.kill("SIGTERM");
        await new Promise((r) => proc2.on("exit", r));
      }
      row = await jobRow(p.id, "j-gk");
    }
    if (row.status === "lost") {
      assert.ok((row.result as Record<string, unknown>)?.observed_outcome,
        "lost verdict carries the honest observed outcome");
    } else {
      assert.ok(row.status === "failed" || row.status === "cancelled",
        `honest terminal verdict, got ${row.status}`);
    }
  } finally {
    proc.kill("SIGKILL");
  }
});

test("reconcile reaps an exec'd orphan via its surviving process group — the leak no descendant walk reaches (F391)", { timeout: 30_000 }, async () => {
  const p = await makePersona("grp-reap");
  await submitJob(p.id, "j-grp", { code: 'export async function run(){ return 1 }' });
  const { claimed } = await client.claimJobs(p.id, RUNNER_ID, 30_000, 1);
  assert.ok(claimed.some((j) => j.job_id === "j-grp"));
  const wd = mkdtempSync(join(tmpdir(), "sumi-rec-grp-"));
  const runner = new Runner({
    api: REAL_API, token: RUNTIME, runnerID: RUNNER_ID, workDir: wd,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 1, cgroupMode: "prlimit", log: () => {},
  });
  // Construct the exact j-pp leak shape: a detached group whose LEADER
  // dies while a member keeps running — the member reparents to init,
  // invisible to every descendant walk, but still carries pgrp == the
  // dead leader's pid. A sleep binary named "workerd" stands in for the
  // payload; only its comm matters to the reaper.
  const fakeWorkerd = join(wd, "workerd");
  writeFileSync(fakeWorkerd, readFileSync("/bin/sleep"), { mode: 0o755 });
  const leader = spawn("/bin/sh", ["-c", `${fakeWorkerd} 60 & exec /bin/sleep 60`], { detached: true });
  await waitFor(() => childrenOf(leader.pid!).length > 0, 5_000, "payload forked in the group");
  const member = childrenOf(leader.pid!)[0]!;
  leader.kill("SIGKILL");
  await waitFor(() => notRunning(leader.pid!), 5_000, "group leader dead");
  assert.ok(!notRunning(member), "the orphaned payload is still running — the leak to reap");
  // Seed the journal exactly as a pre-ready crash left it: the recorded
  // pid is the wrapper/leader — the workerd resolution never persisted.
  const j = runner.journal.create({
    job_id: "j-grp", persona_id: p.id, runner_id: RUNNER_ID,
    pid: leader.pid!, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: null, stats_path: null, cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "x" }, usage_fact_id: `script:j-grp:exec`,
  });
  runner.journal.update(j, { status: "spawned", spawned_at: new Date().toISOString() });
  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, log: () => {} });
  await rec.run();
  // THE invariant: the exec'd orphan is dead — the surviving process
  // group was the only reach that could touch it.
  await waitFor(() => notRunning(member), 5_000, "orphaned group member reaped via pgid");
  assert.equal(workerdPids().length, 0, "no workerd survives anywhere");
});
