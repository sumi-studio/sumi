/**
 * End-to-end: real state-dev API on real PostgreSQL, real workerd per
 * job under runlimited, real file ops through the API's job-scoped
 * routes to an in-process filesvc stub implementing the actual
 * /v1/files protocol with committed-receipt idempotency.
 *
 * Harness boundaries: filesvc is a protocol stub (Map-backed, correct
 * keyed-receipt semantics), not JuiceFS; everything else — the durable
 * job lifecycle, the ledger, workerd, rlimits, rusage — is real.
 *
 * Runs inside the fixture container (docker, bridge network) where PG,
 * the API and the supervisor share 127.0.0.1. Env:
 *   SUMI_TEST_DB_URL   postgres://postgres:sumi-test@172.17.0.5:5432/sumi_scripts_impl
 *   STATE_DEV_BIN      /src/artifacts/bin/state-dev
 *   WORKERD_BIN        /src/artifacts/bin/workerd
 *   RUNLIMITED_BIN     /src/apps/scripts/bin/runlimited
 */

import { test, before, after } from "node:test";
import * as assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as http from "node:http";

import { Runner, defaultDispatcherPath } from "../src/runner.ts";
import { Reconciler } from "../src/reconcile.ts";
import { StateClient } from "../src/api.ts";
import { spawnJob } from "../src/spawn.ts";
import { alive } from "../src/proc.ts";
import { writeConfig } from "../src/workerd/config.ts";
import { StubFileSvc } from "./filesvc_stub.mts";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? "postgres://postgres:sumi-test@172.17.0.5:5432/sumi_scripts_impl?sslmode=disable";
const STATE_DEV_BIN = process.env.STATE_DEV_BIN ?? "/src/artifacts/bin/state-dev";
const WORKERD_BIN = process.env.WORKERD_BIN ?? "/src/artifacts/bin/workerd";
const RUNLIMITED_BIN = process.env.RUNLIMITED_BIN ?? "/src/apps/scripts/bin/runlimited";

const ADMIN = "integration-admin-token-0123456789abcdef";
const RUNTIME = "integration-runtime-token-0123456789abcdef";
const API = "http://127.0.0.1:8180";
const RUNNER_ID = `it-runner-${process.pid}`;

const stub = new StubFileSvc();
let apiProc: ReturnType<typeof spawn> | null = null;
let workDir = "";
let persona = "";
let client: StateClient;
let runner: Runner;
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

async function submitJob(jobID: string, request: unknown): Promise<void> {
  await adminCall("POST", `/internal/core/personas/${persona}/jobs`, {
    job_id: jobID, kind: "script", request,
  });
}

async function jobRow(jobID: string): Promise<Record<string, unknown>> {
  const { job } = await adminCall("GET", `/internal/core/personas/${persona}/jobs/${jobID}`) as { job: Record<string, unknown> };
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

async function runOne(jobID: string, request: unknown): Promise<Record<string, unknown>> {
  await submitJob(jobID, request);
  const claimed = await runner.claimPersona(persona);
  const job = claimed.find((j) => j.job_id === jobID);
  assert.ok(job, `job ${jobID} claimed`);
  await runner.runJob(job);
  return jobRow(jobID);
}

before(async () => {
  const stubURL = await stub.start();
  workDir = mkdtempSync(join(tmpdir(), "sumi-scripts-it-"));

  apiProc = spawn(STATE_DEV_BIN, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: DB_URL,
      SUMI_DB_CREATE: "1",
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
      SUMI_STATE_LISTEN: "127.0.0.1:8180",
      SUMI_FILESVC_URL: stubURL,
      SUMI_FILESVC_TOKEN: "svc-token",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  apiProc.stdout?.on("data", (d) => apiLogs.push(String(d)));
  apiProc.stderr?.on("data", (d) => apiLogs.push(String(d)));
  apiProc.on("exit", (c, s) => apiLogs.push(`state-dev exited code=${c} sig=${s}`));

  await waitFor(async () => {
    try {
      const res = await fetch(`${API}/internal/core/personas`, { method: "OPTIONS" });
      return res.status > 0;
    } catch { return false; }
  }, 15000, "state-dev listen");

  persona = uuidv7();
  await adminCall("POST", "/internal/core/personas", { persona_id: persona, display_name: "it" });

  client = new StateClient({ api: API, token: RUNTIME });
  runner = new Runner({
    api: API, token: RUNTIME, runnerID: RUNNER_ID,
    workDir, workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: defaultDispatcherPath(),
    leaseMs: 10_000, heartbeatMs: 500, claimLimit: 4,
    cgroupMode: "prlimit",
    log: (l) => apiLogs.push(l),
  });
});

after(async () => {
  apiProc?.kill("SIGKILL");
  await stub.stop();
  try { rmSync(workDir, { recursive: true, force: true }); } catch { /* keep evidence on failure */ }
});

test("success: script returns value, logs, measured usage", { timeout: 60_000 }, async () => {
  const row = await runOne("j-ok", {
    code: `export async function run(input, sumi) {
      await sumi.log("hello from script");
      return { doubled: input.n * 2 };
    }`,
    input: { n: 21 },
    limits: { cpu_seconds: 10, wall_ms: 20_000 },
  });
  assert.equal(row.status, "done");
  const result = row.result as Record<string, unknown>;
  assert.deepEqual(result.value, { doubled: 42 });
  const logs = result.logs as { lines: string[] };
  assert.ok(logs.lines.includes("hello from script"));
  const usage = result.usage as Record<string, unknown>;
  assert.equal(typeof usage.cpu_ms, "number", "rusage cpu measured");
  assert.equal(usage.cpu_ms_source, "measured");
  assert.equal(usage.memory_enforcement, "v8_heap_bound");
  assert.equal(result.file_ops_pending, 0);
});

test("files: write then read through the real authority chain", { timeout: 60_000 }, async () => {
  const row = await runOne("j-files", {
    code: `export async function run(input, sumi) {
      await sumi.files.write("out/result.txt", "payload:" + input.msg);
      const r = await sumi.files.read("out/result.txt");
      const st = await sumi.files.stat("out/result.txt");
      return { text: r.text, size: st.size };
    }`,
    input: { msg: "hi" },
    limits: { cpu_seconds: 10, wall_ms: 20_000 },
  });
  assert.equal(row.status, "done");
  const result = row.result as Record<string, unknown>;
  assert.deepEqual(result.value, { text: "payload:hi", size: 10 });
  // The ledger recorded the write as settled.
  const { ops } = await client.listFileOps(persona, "j-files");
  assert.equal(ops.length, 1);
  assert.equal(ops[0]!.status, "settled");
  assert.equal(ops[0]!.path, "out/result.txt");
});

test("cross-persona: file op route rejects a foreign claim", { timeout: 30_000 }, async () => {
  // Persona B's runner cannot operate on persona A's job — the route
  // derives scope from the URL persona and verifies the recorded claim.
  const other = uuidv7();
  await adminCall("POST", "/internal/core/personas", { persona_id: other, display_name: "other" });
  const res = await fetch(`${API}/internal/core/personas/${other}/jobs/j-files/files/read`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${RUNTIME}` },
    body: JSON.stringify({ runner_id: RUNNER_ID, path: "out/result.txt" }),
  });
  assert.equal(res.status, 404); // job not found under that persona
});

test("busy CPU loop: kernel kill at the RLIMIT_CPU bound, sibling responsive", { timeout: 90_000 }, async () => {
  await submitJob("j-busy", {
    code: `export async function run() { for (;;) {} }`,
    limits: { cpu_seconds: 3, wall_ms: 60_000 },
  });
  await submitJob("j-sibling", {
    code: `export async function run(input) { return { ok: input.x }; }`,
    input: { x: 7 },
    limits: { cpu_seconds: 10, wall_ms: 20_000 },
  });
  const claimed = await runner.claimPersona(persona);
  const busy = claimed.find((j) => j.job_id === "j-busy")!;
  const sibling = claimed.find((j) => j.job_id === "j-sibling")!;
  assert.ok(busy && sibling);
  const [bRow] = await Promise.all([
    runner.runJob(busy).then(() => jobRow("j-busy")),
    runner.runJob(sibling).then(() => jobRow("j-sibling")),
  ]);
  const sRow = await jobRow("j-sibling");
  assert.equal(sRow.status, "done");
  assert.equal(bRow.status, "failed");
  const bu = (bRow.result as Record<string, unknown>).usage as Record<string, unknown>;
  // runlimited sets RLIMIT_CPU soft=3s, hard=4s: normally SIGXCPU kills at
  // the soft bound; under scheduling contention the worker can ride the
  // 1s grace into the hard-limit SIGKILL. Either way the kernel enforced
  // the CPU bound — the measured cpu_ms window is the real assertion.
  assert.ok(bu.exit_signal === "SIGXCPU" || bu.exit_signal === "SIGKILL",
    `expected kernel CPU-limit kill, got ${bu.exit_signal}`);
  assert.ok((bu.cpu_ms as number) >= 2900 && (bu.cpu_ms as number) <= 4300, `cpu ~3s, got ${bu.cpu_ms}`);
});

test("wall timeout: a sleeping script is killed at wall_ms", { timeout: 60_000 }, async () => {
  const t0 = Date.now();
  const row = await runOne("j-sleep", {
    code: `export async function run() { await new Promise(r => setTimeout(r, 60000)); return 1; }`,
    limits: { cpu_seconds: 30, wall_ms: 3_000 },
  });
  assert.ok(Date.now() - t0 < 30_000);
  assert.equal(row.status, "failed");
  const result = row.result as Record<string, unknown>;
  assert.equal(result.reason, "wall_timeout");
});

test("memory hog: worker dies, job failed, sibling unaffected", { timeout: 120_000 }, async (t) => {
  // A memory hog is only safe to run when a REAL bound enforces the
  // limit — systemd mode's cgroup MemoryMax constrains memory use.
  // Under prlimit the only backstop is V8 heap accounting, which does
  // NOT bound external ArrayBuffer growth: this exact 64MiB-chunk hog
  // reached ~44 GiB anonymous RSS and triggered the host's global OOM
  // killer before any abort fired (2026-09-18 pa02 incident). An
  // unbounded hog on an uncapped process is a host-level hazard, not a
  // test — skip honestly rather than accept either outcome.
  if (runner.cfg.cgroupMode !== "systemd") {
    t.skip(`no effective memory bound under ${runner.cfg.cgroupMode} — run under systemd MemoryMax instead`);
    return;
  }
  const row = await runOne("j-mem", {
    code: `export async function run() {
      const chunks = [];
      for (;;) chunks.push(new ArrayBuffer(64 * 1024 * 1024));
    }`,
    limits: { cpu_seconds: 60, wall_ms: 60_000 },
  });
  assert.equal(row.status, "failed");
  const result = row.result as Record<string, unknown>;
  assert.ok(
    result.reason === "worker_error" || result.reason === "wall_timeout",
    `expected worker_error or wall_timeout, got ${result.reason}`,
  );
});

test("cancel: a running script is killed and the job ends cancelled", { timeout: 60_000 }, async () => {
  await submitJob("j-cancel", {
    code: `export async function run() { await new Promise(r => setTimeout(r, 60000)); return 1; }`,
    limits: { cpu_seconds: 30, wall_ms: 60_000 },
  });
  const claimed = await runner.claimPersona(persona);
  const job = claimed.find((j) => j.job_id === "j-cancel")!;
  const running = runner.runJob(job);
  await new Promise((r) => setTimeout(r, 2500)); // let it spawn+dispatch
  await adminCall("POST", `/internal/core/personas/${persona}/jobs/j-cancel/cancel`);
  await running;
  const row = await jobRow("j-cancel");
  assert.equal(row.status, "cancelled");
});

test("cancel mid-write: the admitted op still settles via keyed resend", { timeout: 90_000 }, async () => {
  // The stub delays the write 2.5s; cancel lands while the upstream
  // commit is in flight. The worker is killed — the ledger row stays
  // unresolved until finish() resends the same key, settling it.
  stub.faults.delayMs = 2500;
  await submitJob("j-cancel-write", {
    code: `export async function run(input, sumi) {
      await sumi.files.write("later.txt", "committed-anyway");
      return "done";
    }`,
    limits: { cpu_seconds: 30, wall_ms: 60_000 },
  });
  const claimed = await runner.claimPersona(persona);
  const job = claimed.find((j) => j.job_id === "j-cancel-write")!;
  const running = runner.runJob(job);
  await waitFor(async () => {
    const { ops } = await client.listFileOps(persona, "j-cancel-write");
    return ops.length === 1;
  }, 15000, "file op admitted");
  await adminCall("POST", `/internal/core/personas/${persona}/jobs/j-cancel-write/cancel`);
  await running;
  stub.faults.delayMs = 0;
  const row = await jobRow("j-cancel-write");
  assert.equal(row.status, "cancelled");
  const { ops } = await client.listFileOps(persona, "j-cancel-write");
  assert.equal(ops[0]!.status, "settled", `op settled via resolve, got ${ops[0]!.status}`);
  const scope = Object.keys(Object.fromEntries(stub.files))[0];
  const files = stub.scopeFiles(scope!);
  assert.equal(files.get("later.txt")?.body.toString(), "committed-anyway");
});

test("lost upstream response: op unknown then settled as replayed", { timeout: 90_000 }, async () => {
  // Stub commits the write but destroys the connection: the API records
  // 'unknown' (honest) while the effect is real. finish()'s resend is
  // dropped too (flag still armed), so the terminal row honestly reports
  // file_ops_pending=1. Clearing the fault and re-resolving settles it
  // as a replayed receipt — the durable path works across failures.
  stub.faults.dropResponse = true;
  await submitJob("j-lost", {
    code: `export async function run(input, sumi) {
      await sumi.files.write("maybe.txt", "did-commit");
      return "done";
    }`,
    limits: { cpu_seconds: 30, wall_ms: 60_000 },
  });
  const claimed = await runner.claimPersona(persona);
  const job = claimed.find((j) => j.job_id === "j-lost")!;
  await runner.runJob(job);
  const row = await jobRow("j-lost");
  assert.ok(["failed", "done"].includes(row.status as string));
  let { ops } = await client.listFileOps(persona, "j-lost");
  assert.equal(ops[0]!.status, "unknown");
  assert.equal((row.result as Record<string, unknown>).file_ops_pending, 1);

  stub.faults.dropResponse = false;
  const { op } = await client.resolveFileOp(persona, "j-lost", ops[0]!.op_id, RUNNER_ID);
  assert.equal(op.status, "settled");
  ({ ops } = await client.listFileOps(persona, "j-lost"));
  assert.equal((ops[0]!.result as Record<string, unknown>)?.replayed, true);
});

test("cancel: admitted write settles, post-cancel calls never happen", { timeout: 60_000 }, async () => {
  // The first write is admitted before cancellation and settles; the
  // worker is killed so the post-sleep write is never even attempted —
  // and the authority gate would deny it anyway (Go-side tests).
  await submitJob("j-denied", {
    code: `export async function run(input, sumi) {
      await sumi.files.write("first.txt", "ok");
      await new Promise(r => setTimeout(r, 4000));
      await sumi.files.write("should-not.txt", "x");
      return "unreachable";
    }`,
    limits: { cpu_seconds: 30, wall_ms: 60_000 },
  });
  const claimed = await runner.claimPersona(persona);
  const job = claimed.find((j) => j.job_id === "j-denied")!;
  const running = runner.runJob(job);
  // Deterministic: the first write must be settled before the cancel
  // lands, or the kill races the upstream call and the op is honestly
  // 'unknown' instead (a different, also-valid outcome).
  await waitFor(async () => {
    const { ops } = await client.listFileOps(persona, "j-denied");
    return ops.length === 1 && ops[0]!.status === "settled";
  }, 15000, "first write settled");
  await adminCall("POST", `/internal/core/personas/${persona}/jobs/j-denied/cancel`);
  await running;
  const row = await jobRow("j-denied");
  assert.equal(row.status, "cancelled");
  const { ops } = await client.listFileOps(persona, "j-denied");
  assert.equal(ops.length, 1);
  assert.equal(ops[0]!.path, "first.txt");
  assert.equal(ops[0]!.status, "settled");
});

test("reconcile: orphan workerd reaped by verified identity, no re-exec", { timeout: 60_000 }, async () => {
  // Simulate a dead supervisor: a live workerd + a journal saying
  // 'running'. The reconciler must kill exactly that process and never
  // re-execute the job.
  const jobDir = mkdtempSync(join(workDir, "orphan-"));
  const socketPath = join(workDir, "sock", "j-orphan.sock");
  const statsPath = join(workDir, "tmp", "j-orphan.rusage");
  const configPath = writeConfig(jobDir, {
    socketPath, dispatcherPath: defaultDispatcherPath(),
    api: API, token: RUNTIME, personaID: persona, jobID: "j-orphan", runnerID: RUNNER_ID,
    limits: { cpu_ms: 60_000, file_calls: 10, file_bytes: 1 << 20, log_bytes: 4096 },
  });
  const spawned = spawnJob({
    configPath, socketPath, statsPath,
    cpuSeconds: 60, memoryMib: 256, cgroupMode: "prlimit",
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
  }, () => {});
  const j = runner.journal.create({
    job_id: "j-orphan", persona_id: persona, runner_id: RUNNER_ID,
    pid: spawned.pid, start_ticks: null, boot_id: null,
    unit_name: null, socket_path: socketPath, stats_path: statsPath,
    cgroup_mode: "prlimit", limits: {},
    spec: { code_sha256: "x" }, usage_fact_id: "script:j-orphan:exec",
  });
  runner.journal.update(j, { status: "running" });
  // Give workerd a moment to boot inside runlimited.
  await new Promise((r) => setTimeout(r, 800));
  assert.ok(alive(spawned.pid), "orphan process alive before reconcile");

  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, workerdBin: WORKERD_BIN });
  await rec.run();
  await new Promise((r) => setTimeout(r, 300));
  assert.ok(!alive(spawned.pid), "orphan killed by verified identity");
  const after = runner.journal.read("j-orphan")!;
  assert.equal(after.status, "reaped");
});

test("journal survives: exited result re-reported after lost complete", { timeout: 60_000 }, async () => {
  // A journal whose status is 'exited' with a stored result re-reports
  // idempotently — the job never re-executes.
  const j = runner.journal.create({
    job_id: "j-replay", persona_id: persona, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: null, stats_path: null, cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "x" }, usage_fact_id: "script:j-replay:exec",
  });
  runner.journal.update(j, {
    status: "exited",
    result: { terminal_status: "done", value: 1, usage: { cpu_ms: 42 } },
  });
  await submitJob("j-replay", { code: "export async function run(){return 1}" });
  // Claim it ourselves so the row is ours to complete.
  const claimed = await runner.claimPersona(persona);
  assert.ok(claimed.some((c) => c.job_id === "j-replay"));
  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, workerdBin: WORKERD_BIN });
  await rec.run();
  const row = await jobRow("j-replay");
  assert.equal(row.status, "done", "re-reported terminal outcome landed");
});

function postUnix(socketPath: string, path: string, body?: unknown): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? undefined : JSON.stringify(body);
    const req = http.request(
      {
        socketPath, path, method: body === undefined ? "GET" : "POST",
        headers: payload === undefined ? {} : { "content-type": "application/json", "content-length": Buffer.byteLength(payload) },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8"))); }
          catch (e) { reject(e); }
        });
      },
    );
    req.on("error", reject);
    if (payload !== undefined) req.write(payload);
    req.end();
  });
}

test("reconcile: sleeping low-CPU orphan reaped by identity, no re-exec", { timeout: 60_000 }, async () => {
  // A workerd mid-dispatch on a sleeping (near-zero-CPU) script: the
  // busy-loop case only proves a hot process dies. This orphan is doing
  // nothing — the reaper must still find and kill it by identity.
  const jobDir = mkdtempSync(join(workDir, "orphan-sleep-"));
  const socketPath = join(workDir, "sock", "j-orphan-sleep.sock");
  const statsPath = join(workDir, "tmp", "j-orphan-sleep.rusage");
  const configPath = writeConfig(jobDir, {
    socketPath, dispatcherPath: defaultDispatcherPath(),
    api: API, token: RUNTIME, personaID: persona, jobID: "j-orphan-sleep", runnerID: RUNNER_ID,
    limits: { cpu_ms: 60_000, file_calls: 10, file_bytes: 1 << 20, log_bytes: 4096 },
  });
  const spawned = spawnJob({
    configPath, socketPath, statsPath,
    cpuSeconds: 60, memoryMib: 256, cgroupMode: "prlimit",
    workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
  }, () => {});
  await waitFor(async () => {
    try { await postUnix(socketPath, "/ready"); return true; } catch { return false; }
  }, 15000, "orphan workerd ready");
  // Dispatch a 60s sleep — the worker is now mid-run at ~0% CPU.
  const sleeping = postUnix(socketPath, "/run", {
    code: `export async function run() { await new Promise(r => setTimeout(r, 60000)); return 1; }`,
  }).catch(() => ({})); // socket dies with the worker — that is the point

  const j = runner.journal.create({
    job_id: "j-orphan-sleep", persona_id: persona, runner_id: RUNNER_ID,
    pid: spawned.pid, start_ticks: null, boot_id: null,
    unit_name: null, socket_path: socketPath, stats_path: statsPath,
    cgroup_mode: "prlimit", limits: {},
    spec: { code_sha256: "x" }, usage_fact_id: "script:j-orphan-sleep:exec",
  });
  runner.journal.update(j, { status: "running" });

  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, workerdBin: WORKERD_BIN });
  await rec.run();
  await sleeping;
  await new Promise((r) => setTimeout(r, 300));
  assert.ok(!alive(spawned.pid), "sleeping orphan killed by verified identity");
  const after = runner.journal.read("j-orphan-sleep")!;
  assert.equal(after.status, "reaped");
});

test("swept to lost: verdict preserved, pending op resolves, usage attaches", { timeout: 60_000 }, async () => {
  // A runner whose claim expires mid-write is swept to 'lost' — an
  // immutable verdict. The pending file op must still settle through
  // the keyed resend (never replayed as a fresh op), the observed usage
  // must still attach through the persona-scoped usage ledger, and the
  // verdict must not be rewritten by CompleteJob.
  await submitJob("j-swept-lost", { code: `export async function run() { return 1 }` });
  // Claim with a short lease; do NOT heartbeat — the claim expires.
  const { claimed } = await client.claimJobs(persona, RUNNER_ID, 2_500, 4);
  assert.ok(claimed.some((j) => j.job_id === "j-swept-lost"), "j-swept-lost claimed");

  // While the claim is live, admit a write whose upstream response is
  // destroyed after commit: the ledger records 'unknown' — the honest
  // state when the effect cannot be confirmed.
  stub.faults.dropResponse = true;
  const writeRes = await fetch(`${API}/internal/core/personas/${persona}/jobs/j-swept-lost/files/write`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${RUNTIME}` },
    body: JSON.stringify({ runner_id: RUNNER_ID, path: "lost-write.txt", data_base64: Buffer.from("committed").toString("base64") }),
  });
  stub.faults.dropResponse = false;
  const writeBody = await writeRes.json() as { op: { status: string } };
  assert.equal(writeBody.op.status, "unknown", `admitted op recorded unknown, got ${writeBody.op.status}`);

  // Let the lease expire, then trigger the sweep by claiming again —
  // ClaimJobs' kind-blind sweep marks the expired running claim 'lost'.
  // A slow first-claim response can compress the effective wait below
  // the lease, so sweep-and-poll is retried to a deadline, not slept once.
  await submitJob("j-sweep-trigger", { code: `export async function run() { return 1 }` });
  await new Promise((r) => setTimeout(r, 500));
  let lostRow = await jobRow("j-swept-lost");
  const sweepDeadline = Date.now() + 10_000;
  while (lostRow.status !== "lost") {
    if (Date.now() > sweepDeadline) throw new Error(`j-swept-lost not swept to lost: ${lostRow.status}`);
    await client.claimJobs(persona, RUNNER_ID, 10_000, 4);
    await new Promise((r) => setTimeout(r, 500));
    lostRow = await jobRow("j-swept-lost");
  }
  assert.equal(lostRow.status, "lost", "expired claim swept to lost");
  assert.equal((lostRow.result as Record<string, unknown>).reason, "claim_expired");

  // The journaled evidence a dead supervisor left behind: exited with a
  // measured rusage, never reported.
  const j = runner.journal.create({
    job_id: "j-swept-lost", persona_id: persona, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: null, stats_path: null, cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "x" }, usage_fact_id: `script:j-swept-lost:exec`,
  });
  runner.journal.update(j, {
    status: "exited",
    result: {
      terminal_status: "failed", reason: "worker_killed",
      usage: { cpu_ms: 1234, cpu_ms_source: "measured", max_rss_bytes: 50_000_000, wall_ms: 2500 },
    },
  });

  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, workerdBin: WORKERD_BIN });
  await rec.run();

  // 1. The verdict is immutable: still 'lost', still the sweep's result.
  const after = await jobRow("j-swept-lost");
  assert.equal(after.status, "lost", "lost verdict preserved, not rewritten");
  assert.equal((after.result as Record<string, unknown>).reason, "claim_expired");

  // 2. The pending op settled through the keyed resend — same op_id,
  //    replayed receipt, no second effect.
  const { ops, pending } = await client.listFileOps(persona, "j-swept-lost");
  assert.equal(ops.length, 1, "one durable op — resend is not a fresh operation");
  assert.equal(ops[0]!.status, "settled", `op settled, got ${ops[0]!.status}`);
  assert.equal((ops[0]!.result as Record<string, unknown>).replayed, true, "receipt proves single commit");
  assert.equal(pending, 0, "no permanent pending bookkeeping");

  // 3. Measured usage attached via the persona-scoped usage ledger —
  //    RecordUsage is not claim-fenced, so the evidence lands even on a
  //    lost job.
  const jAfter = runner.journal.read("j-swept-lost")!;
  assert.equal(jAfter.usage_status, "recorded", "measured usage fact delivered");

  // 4. The shared AttachLostOutcome route IS wired on the merged
  //    producer: the observed outcome attaches under
  //    result.observed_outcome WITHOUT rewriting the 'lost' verdict.
  const attached = (after.result as Record<string, unknown>).observed_outcome as Record<string, unknown> | undefined;
  assert.ok(attached, "observed outcome attached to the lost row");
  assert.equal(attached.observed_status, "failed");
  assert.equal((attached.result as Record<string, unknown>).reason, "worker_killed");
  assert.ok(
    jAfter.notes.some((n) => n.includes("observed outcome attached")),
    "journal notes the attach landed through the shared route",
  );
});

test("crash mid-write: reconciler settles op and reports failure, no re-exec", { timeout: 60_000 }, async () => {
  // Supervisor dies after the op was admitted but before the response
  // settled — the 'running' claim is still live when the reconciler
  // starts. The op resolves through the keyed resend, the job reports
  // 'failed' with an honest file_ops_pending count, and usage is sent.
  await submitJob("j-crash", { code: `export async function run() { return 1 }` });
  const { claimed } = await client.claimJobs(persona, RUNNER_ID, 15_000, 4);
  assert.ok(claimed.some((j) => j.job_id === "j-crash"));

  stub.faults.dropResponse = true;
  await fetch(`${API}/internal/core/personas/${persona}/jobs/j-crash/files/write`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${RUNTIME}` },
    body: JSON.stringify({ runner_id: RUNNER_ID, path: "crash.txt", data_base64: Buffer.from("x").toString("base64") }),
  });
  stub.faults.dropResponse = false;
  const { pending: midPending } = await client.listFileOps(persona, "j-crash", true);
  assert.equal(midPending, 1, "op unresolved while supervisor dead");

  // The supervisor's journal at death: 'running', evidence not yet
  // journaled — the reconciler must not fabricate measured usage.
  const j = runner.journal.create({
    job_id: "j-crash", persona_id: persona, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: null, stats_path: null, cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "x" }, usage_fact_id: "script:j-crash:exec",
  });
  runner.journal.update(j, { status: "running" });

  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, workerdBin: WORKERD_BIN });
  await rec.run();

  const { ops, pending } = await client.listFileOps(persona, "j-crash");
  assert.equal(ops.length, 1);
  assert.equal(ops[0]!.status, "settled", `crash op settled via resend, got ${ops[0]!.status}`);
  assert.equal((ops[0]!.result as Record<string, unknown>).replayed, true);
  assert.equal(pending, 0);
  const jAfter = runner.journal.read("j-crash")!;
  assert.equal(jAfter.file_ops_pending, 0, "journal records the settled count");
});

test("usage boundary: exited-without-rusage reports unknown, not fabricated", { timeout: 60_000 }, async () => {
  // A supervisor that died after the process exited but before wait4
  // evidence was journaled has NO measured usage — the fact must go out
  // 'unknown', never a fabricated zero or estimate. A measured sibling
  // proves the same code path reports when evidence exists.
  const j1 = runner.journal.create({
    job_id: "j-usage-unknown", persona_id: persona, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: null, stats_path: null, cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "x" }, usage_fact_id: "script:j-usage-unknown:exec",
  });
  runner.journal.update(j1, {
    status: "exited",
    result: { terminal_status: "failed", reason: "worker_error", usage: { cpu_ms: null, cpu_ms_source: "unknown" } },
  });
  const j2 = runner.journal.create({
    job_id: "j-usage-measured", persona_id: persona, runner_id: RUNNER_ID,
    pid: null, start_ticks: null, boot_id: null, unit_name: null,
    socket_path: null, stats_path: null, cgroup_mode: "prlimit",
    limits: {}, spec: { code_sha256: "x" }, usage_fact_id: "script:j-usage-measured:exec",
  });
  runner.journal.update(j2, {
    status: "exited",
    result: { terminal_status: "done", value: 1, usage: { cpu_ms: 88, cpu_ms_source: "measured" } },
  });

  const rec = new Reconciler({ client, journal: runner.journal, runnerID: RUNNER_ID, workerdBin: WORKERD_BIN });
  await rec.run();

  assert.equal(runner.journal.read("j-usage-unknown")!.usage_status, "unknown",
    "unmeasured usage sent as unknown");
  assert.equal(runner.journal.read("j-usage-measured")!.usage_status, "recorded",
    "measured usage sent as reported");
});
