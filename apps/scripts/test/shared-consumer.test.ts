/**
 * Shared-seam consumer integration: the script runner against the REAL
 * producer state-dev — the binary built from the read-only
 * linux-on-demand checkout — on real PostgreSQL.
 *
 * Covers the consumer half of the shared contract:
 *   GET /internal/core/jobs/runnable   — dynamic persona discovery
 *   GET /internal/core/jobs/attention  — this runner's unresolved work
 *   POST .../jobs/{j}/lost-outcome     — observed outcome on a lost row
 * plus ordinary claim/heartbeat/complete/usage through the same API.
 *
 * On the merged producer every job state is produced through the real
 * API surface — admission via POST /jobs, 'running' via the claim route,
 * 'lost'/'cancel_requested' via short-lease expiry + the claim-tx sweep
 * and the cancel route. No SQL seeding: the producer's own transitions
 * are the fixture. File effects run through the real jobfiles routes
 * against an in-process filesvc protocol stub (keyed committed-receipt
 * idempotency). cgroup mode is prlimit here — the systemd scope proof
 * is a separate host gate already verified.
 *
 * Env:
 *   PRODUCER_STATE_DEV_BIN   path to the built producer binary
 *   SUMI_TEST_DB_URL         postgres://... (a fresh randomly suffixed
 *                            db is derived from it — per-run isolation)
 *   WORKERD_BIN              workerd binary
 *   RUNLIMITED_BIN           runlimited binary
 */

import { test, before, after } from "node:test";
import * as assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Runner, defaultDispatcherPath } from "../src/runner.ts";
import { Reconciler } from "../src/reconcile.ts";
import { Journal } from "../src/journal.ts";
import { SharedDiscovery, StateClient } from "../src/api.ts";
import { StubFileSvc } from "./filesvc_stub.mts";

const req = (k: string): string => {
  const v = process.env[k];
  if (!v) throw new Error(`${k} required`);
  return v;
};
// A fresh database per run replaces the old TRUNCATE — a random suffix,
// not pid (container pids are constant across runs and reuse the name).
const RUN_SUFFIX = crypto.randomUUID().slice(0, 8);
const DB_URL = req("SUMI_TEST_DB_URL").replace(/\/[^/?]+(\?.*)?$/, `/sumi_consumer_${RUN_SUFFIX}$1`);
const STATE_DEV_BIN = process.env.PRODUCER_STATE_DEV_BIN ?? "";
const WORKERD_BIN = req("WORKERD_BIN");
const RUNLIMITED_BIN = req("RUNLIMITED_BIN");

const ADMIN = "consumer-admin-token-0123456789abcdef";
const RUNTIME = "consumer-runtime-token-0123456789abcdef";
// When CONSUMER_API is set the producer is already running elsewhere
// (e.g. inside a fixture container) — the test only drives its HTTP.
const EXTERNAL_API = process.env.CONSUMER_API ?? "";
const API = EXTERNAL_API || "http://127.0.0.1:8185";
const RUNNER_ID = `consumer-it-${RUN_SUFFIX}`;

let apiProc: ReturnType<typeof spawn> | null = null;
const stub = new StubFileSvc();
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

async function adminCall(method: string, path: string, body?: unknown): Promise<unknown> {
  const res = await fetch(`${API}${path}`, {
    method,
    headers: { "content-type": "application/json", authorization: `Bearer ${ADMIN}` },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const payload = await res.json().catch(() => null);
  if (!res.ok) throw new Error(`${method} ${path} -> ${res.status}: ${JSON.stringify(payload)}`);
  return payload;
}

async function serviceGet(path: string, token = RUNTIME): Promise<{ status: number; body: unknown }> {
  const res = await fetch(`${API}${path}`, { headers: { authorization: `Bearer ${token}` } });
  return { status: res.status, body: await res.json().catch(() => null) };
}

/** Personas and jobs both go through the real API. */
async function makePersona(name: string): Promise<{ id: string; token: string }> {
  const id = uuidv7();
  const res = await adminCall("POST", "/internal/core/personas", { persona_id: id, display_name: name }) as { persona_token: string };
  return { id, token: res.persona_token };
}

// Job states are produced through the producer's own transitions, never
// SQL: admission via POST /jobs, a live claim via the claim route,
// 'lost' via a short lease + the claim-transaction sweep.
async function submitJob(personaID: string, jobID: string, kind: string, request: unknown): Promise<void> {
  await adminCall("POST", `/internal/core/personas/${personaID}/jobs`, {
    job_id: jobID, kind, request,
  });
}

async function claimToLost(personaID: string, jobID: string): Promise<void> {
  await submitJob(personaID, jobID, "script", { code: "x" });
  await client.claimJobs(personaID, RUNNER_ID, 400, 4);
  // Each later claim pass re-attempts the sweep; retry until the lease
  // has demonstrably expired rather than trusting one fixed sleep —
  // HTTP latency inside a busy fixture can compress a single wait.
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

async function waitFor(predicate: () => Promise<boolean>, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

function newRunner(dir = workDir): Runner {
  return new Runner({
    api: API, token: RUNTIME, runnerID: RUNNER_ID,
    workDir: dir,
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 30_000, heartbeatMs: 300, claimLimit: 4, cgroupMode: "prlimit",
    log: (l) => apiLogs.push(l),
  });
}

function newReconciler(dir = workDir): Reconciler {
  return new Reconciler({ client: newRunner(dir).client, journal: new Journal(dir), runnerID: RUNNER_ID, log: (l) => apiLogs.push(l) });
}

before(async () => {
  workDir = mkdtempSync(join(tmpdir(), "sumi-consumer-it-"));
  client = new StateClient({ api: API, token: RUNTIME });
  const filesvcURL = await stub.start();
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
        SUMI_FILESVC_URL: filesvcURL,
        SUMI_FILESVC_TOKEN: "svc-token",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    apiProc.stdout?.on("data", (d) => apiLogs.push(String(d)));
    apiProc.stderr?.on("data", (d) => apiLogs.push(String(d)));
    apiProc.on("exit", (c, s) => apiLogs.push(`state-dev exited code=${c} sig=${s}`));
  }
  await waitFor(async () => {
    try {
      const res = await fetch(`${API}/internal/core/jobs/runnable?kinds=script`, { headers: { authorization: `Bearer ${RUNTIME}` } });
      return res.status === 200;
    } catch { return false; }
  }, 15000, "producer state-dev listen");
});

after(() => {
  apiProc?.kill();
  stub.stop();
  try { rmSync(workDir, { recursive: true, force: true }); } catch { /* best effort */ }
});

test("dynamic discovery: a newly queued persona script is found with no persona config", async () => {
  const p = await makePersona("dyn");
  await submitJob(p.id, "j-dyn", "script", {
    code: 'export async function run(input){ return input.n + 1; }',
    input: { n: 41 },
    limits: { cpu_seconds: 5, wall_ms: 15000, memory_mib: 64, output_bytes: 4096, log_bytes: 4096, file_calls: 0, file_bytes: 0 },
  });
  const disc = new SharedDiscovery(client, ["script"], 64);
  const personas = await disc.personas();
  assert.ok(personas.includes(p.id), `discovery returned ${JSON.stringify(personas)}`);

  const runner = newRunner();
  const claimed = await runner.claimPersona(p.id);
  const job = claimed.find((j) => j.job_id === "j-dyn");
  assert.ok(job, "claimed the discovered job");
  await runner.runJob(job);
  const row = await jobRow(p.id, "j-dyn");
  assert.equal(row.status, "done");
  assert.equal((row.result as Record<string, unknown>).value, 42);
});

test("fair paging: a queue deeper than one page is walked, not starved", async () => {
  const ids: string[] = [];
  for (let i = 0; i < 3; i++) {
    const p = await makePersona(`page-${i}`);
    await submitJob(p.id, `j-p${i}`, "script", { code: "x" });
    ids.push(p.id);
  }
  const disc = new SharedDiscovery(client, ["script"], 1); // one job per page
  const seen = new Set<string>();
  // Walk until THIS test's personas are all discovered — other queued
  // personas may share the cursor space and must not end the walk early.
  for (let i = 0; i < 24 && !ids.every((id) => seen.has(id)); i++) {
    for (const p of await disc.personas()) seen.add(p);
  }
  for (const id of ids) assert.ok(seen.has(id), `persona ${id} never discovered — starved behind the page`);
});

test("kind filter: non-script queued jobs are not discovered or claimed", async () => {
  const p = await makePersona("kinds");
  await submitJob(p.id, "j-sub", "subprocess", { command: ["/bin/true"] });
  await submitJob(p.id, "j-scr", "script", { code: "x" });
  const page = await client.runnableJobs(["script"], 64);
  const kinds = page.jobs.filter((j) => j.persona_id === p.id).map((j) => j.job_id);
  assert.deepEqual(kinds, ["j-scr"]);
  const claimed = await newRunner().claimPersona(p.id);
  assert.deepEqual(claimed.map((j) => j.job_id), ["j-scr"]);
});

test("persona token cannot reach cross-persona discovery", async () => {
  const p = await makePersona("scope");
  const r = await serviceGet("/internal/core/jobs/runnable?kinds=script", p.token);
  assert.equal(r.status, 401);
  const a = await serviceGet(`/internal/core/jobs/attention?runner_id=${RUNNER_ID}&kinds=script`, p.token);
  assert.equal(a.status, 401);
});

test("expired claim sweeps to lost; observed outcome attaches via the real route", async () => {
  const p = await makePersona("lost");
  await submitJob(p.id, "j-lost-x", "script", {
    code: 'export async function run(){ await new Promise(r=>setTimeout(r,120000)); return 1; }',
    input: null,
    limits: { cpu_seconds: 120, wall_ms: 120000, memory_mib: 64, output_bytes: 4096, log_bytes: 0, file_calls: 0, file_bytes: 0 },
  });
  // Claim on a short lease, never heartbeat, let it expire.
  const claimed = await newRunner().client.claimJobs(p.id, RUNNER_ID, 250, 4);
  assert.equal(claimed.claimed.length, 1);
  // A later claim pass sweeps the expired claim to 'lost'.
  await waitFor(async () => {
    await newRunner().client.claimJobs(p.id, RUNNER_ID, 250, 4).catch(() => null);
    const row = await jobRow(p.id, "j-lost-x");
    return row.status === "lost";
  }, 10000, "sweep to lost");

  // Journal carries the observed outcome (exited, unreported) — the
  // reconciler must attach it via the REAL producer route.
  const journal = new Journal(workDir);
  const j = journal.create({
    job_id: "j-lost-x", persona_id: p.id, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: "", stats_path: "", cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "0".repeat(64) }, usage_fact_id: "script:j-lost-x:exec",
  });
  journal.update(j, {
    status: "exited",
    result: { terminal_status: "failed", reason: "supervisor_restarted", usage: { cpu_ms: 5 } },
    usage_status: "recorded",
  });
  await newReconciler().run();
  const row = await jobRow(p.id, "j-lost-x");
  assert.equal(row.status, "lost", "verdict stays lost — never rewritten");
  const outcome = (row.result as Record<string, unknown>)?.observed_outcome as Record<string, unknown>;
  assert.ok(outcome, `observed_outcome attached: ${JSON.stringify(row.result)}`);
  assert.equal((outcome.result as Record<string, unknown>).reason, "supervisor_restarted");

  // Attention no longer lists the resolved row; identical replay 200s;
  // divergent evidence 409s; a wrong claimant is 403.
  const att = await client.attentionJobs(RUNNER_ID, ["script"], 64);
  assert.ok(!att.jobs.some((r) => r.job_id === "j-lost-x"), "resolved lost row left the attention set");

  // The reconciler's attached payload is the journal's deterministic
  // wire payload (result minus local bookkeeping keys) — replay it
  // verbatim: identical is idempotent, anything else conflicts.
  const replay = await client.attachLostOutcome(p.id, "j-lost-x", RUNNER_ID, {
    observed_status: "failed",
    result: { reason: "supervisor_restarted", usage: { cpu_ms: 5 } },
    error: "",
  });
  assert.equal(replay.status, 200, "identical replay is idempotent");
});

test("lost-outcome attach: divergent evidence 409, wrong claimant 403", async () => {
  const p = await makePersona("conflict");
  await claimToLost(p.id, "j-conf");
  const first = await client.attachLostOutcome(p.id, "j-conf", RUNNER_ID, {
    observed_status: "failed", result: { reason: "a" }, error: "",
  });
  assert.equal(first.status, 200);
  const divergent = await client.attachLostOutcome(p.id, "j-conf", RUNNER_ID, {
    observed_status: "failed", result: { reason: "DIFFERENT" }, error: "",
  });
  assert.equal(divergent.status, 409, "conflicting evidence refused");
  const wrong = await client.attachLostOutcome(p.id, "j-conf", "someone-else", {
    observed_status: "failed", result: { reason: "a" }, error: "",
  });
  assert.equal(wrong.status, 403, "wrong claimant refused");
  const row = await jobRow(p.id, "j-conf");
  assert.equal(row.status, "lost");
});

test("attention: a running claim with no journal is deferred, then resolved via the lost sweep", async () => {
  const p = await makePersona("nojrn");
  // A claim on a short lease — the same expiry path as a real outage.
  await submitJob(p.id, "j-noj", "script", { code: "x" });
  await client.claimJobs(p.id, RUNNER_ID, 400, 4);
  const wd = mkdtempSync(join(tmpdir(), "sumi-empty-wd-"));
  const res = await newReconciler(wd).run();
  // A missing journal is NOT proof the process stopped — the claim is
  // left for the lease-expiry sweep, never terminalized on absent
  // evidence.
  let row = await jobRow(p.id, "j-noj");
  assert.equal(row.status, "running", "indeterminate claim is not terminalized");
  assert.ok(res.attention.seen >= 1);
  assert.ok(res.attention.pending >= 1, "deferred row reported as pending, not resolved");

  // The lease expires; a later claim call sweeps it to 'lost'.
  await waitFor(async () => {
    await client.claimJobs(p.id, RUNNER_ID, 400, 4).catch(() => null);
    row = await jobRow(p.id, "j-noj");
    return row.status === "lost";
  }, 10_000, "sweep to lost");

  // Now the honest no-evidence outcome attaches through the real route.
  await newReconciler(wd).run();
  row = await jobRow(p.id, "j-noj");
  assert.equal(row.status, "lost", "verdict unchanged");
  const outcome = (row.result as Record<string, unknown>)?.observed_outcome as Record<string, unknown>;
  assert.ok(outcome, "no-evidence outcome attached after the sweep");
  assert.equal((outcome.result as Record<string, unknown>).reason, "runner_restart_no_journal");
});

test("attention: cancel_requested claim with no journal is deferred, not declared cancelled", async () => {
  const p = await makePersona("cancel");
  await submitJob(p.id, "j-cxl", "script", { code: "x" });
  await client.claimJobs(p.id, RUNNER_ID, 30_000, 4);
  await adminCall("POST", `/internal/core/personas/${p.id}/jobs/j-cxl/cancel`);
  await newReconciler(mkdtempSync(join(tmpdir(), "sumi-empty-wd-"))).run();
  const row = await jobRow(p.id, "j-cxl");
  assert.equal(row.status, "cancel_requested",
    "journal absence is not confirmed cancellation — deferred for the sweep");
});

test("attention: lost claim with no journal gets honest no-evidence outcome, not a fabricated verdict", async () => {
  const p = await makePersona("nojrn-lost");
  await claimToLost(p.id, "j-nojl");
  await newReconciler(mkdtempSync(join(tmpdir(), "sumi-empty-wd-"))).run();
  const row = await jobRow(p.id, "j-nojl");
  assert.equal(row.status, "lost", "verdict unchanged");
  const outcome = (row.result as Record<string, unknown>)?.observed_outcome as Record<string, unknown>;
  assert.ok(outcome, "no-evidence outcome attached so the row resolves");
  assert.equal((outcome.result as Record<string, unknown>).reason, "runner_restart_no_journal");
});
