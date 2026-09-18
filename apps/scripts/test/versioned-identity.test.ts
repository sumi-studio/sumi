/**
 * Versioned payload identity — the F400 regression.
 *
 * SUMI_WORKERD_BIN is a PATH; a versioned basename like
 * `workerd-2026-08-04` gives comm `workerd-2026-08` (TASK_COMM_LEN-1),
 * which the literal name "workerd" can never match. Every descendant
 * lookup, pid resolution and ordered kill must derive identity from the
 * configured path, not a fixed name.
 *
 * Harness: the REAL workerd binary reached through a symlink named
 * `workerd-2026-08-04` — the deployment naming shape — spawned through
 * the production spawnJob/runlimited path with real writeConfig capnp.
 * workerd binds its socket and idles; no state API or DB is involved.
 *
 * The file deliberately imports only proc helpers that existed before
 * the fix (alive/commOf/childrenOf/startTicks/bootID/findDescendant):
 * it compiles on the pre-fix tree and fails there BEHAVIORALLY — the
 * payload survives the reap and wait4 stats are lost — rather than
 * failing at an import.
 *
 * Env: RUNLIMITED_BIN + WORKERD_BIN (the suite's existing binaries;
 * the symlink makes the name versioned even when WORKERD_BIN is bare).
 */
import { test, before, after } from "node:test";
import * as assert from "node:assert/strict";
import { existsSync, mkdtempSync, mkdirSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, basename } from "node:path";
import { fileURLToPath } from "node:url";

import { Runner } from "../src/runner.ts";
import { Reconciler } from "../src/reconcile.ts";
import { Journal, type JobJournal, type JournalExit } from "../src/journal.ts";
import { spawnJob, type Spawned } from "../src/spawn.ts";
import { writeConfig } from "../src/workerd/config.ts";
import { alive, commOf, childrenOf, startTicks, bootID, findDescendant } from "../src/proc.ts";
import type { StateClient } from "../src/api.ts";

const RUNLIMITED_BIN = process.env.RUNLIMITED_BIN ?? "/src/apps/scripts/bin/runlimited";
const WORKERD_SRC = process.env.WORKERD_BIN ?? "/src/artifacts/bin/workerd";
const DISPATCHER = fileURLToPath(new URL("../src/workerd/dispatcher.js", import.meta.url));

let wd = "";
let journal: Journal;
let runner: Runner;
/** Versioned-name path to the real workerd: comm is `workerd-2026-08`. */
let versioned = "";
const expectedComm = () => basename(versioned).slice(0, 15);
const spawnedList: Spawned[] = [];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn: () => boolean | Promise<boolean>, timeoutMs: number, what: string) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try { if (await fn()) return; } catch { /* retry */ }
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(50);
  }
}

/** Minimal duck-typed client — reconcile routes only, no HTTP/DB. */
const stubClient = {
  listFileOps: async () => ({ ops: [], pending: 0 }),
  resolveFileOp: async () => ({ op: {} }),
  getJob: async (_p: string, j: string) => ({ job: { job_id: j, status: "running" } }),
  complete: async () => ({ job: {} }),
  recordUsage: async () => ({}),
  sweepExpiredJobs: async () => ({ swept: 0, jobs: [] }),
  attentionJobs: async () => ({ jobs: [] }),
  attachLostOutcome: async () => ({ status: 404, body: null }),
} as unknown as StateClient;

/**
 * Spawn runlimited -> versioned-named real workerd; return once the
 * payload has exec'd (its comm is the truncated versioned basename).
 * `markerInArgv=false` puts the config outside `run-<job>/` so the
 * killOrphanGroup ownership marker cannot fire — isolating the
 * name-vs-derived-identity path under test.
 */
async function spawnVersioned(jobID: string, markerInArgv = true) {
  const cfgDir = join(wd, "tmp", markerInArgv ? `run-${jobID}` : `cfg-${jobID}`);
  mkdirSync(cfgDir, { recursive: true });
  const configPath = writeConfig(cfgDir, {
    socketPath: journal.socketPath(jobID), dispatcherPath: DISPATCHER,
    api: "http://127.0.0.1:1", token: "t", personaID: "p-ver", jobID, runnerID: "ver-it",
    limits: { cpu_ms: 60_000, file_calls: 10, file_bytes: 1 << 20, log_bytes: 4096 },
  });
  const spawned = spawnJob({
    configPath, socketPath: journal.socketPath(jobID), statsPath: journal.statsPath(jobID),
    cpuSeconds: 60, memoryMib: 256, cgroupMode: "prlimit",
    workerdBin: versioned, runlimitedBin: RUNLIMITED_BIN,
  }, () => {});
  spawnedList.push(spawned);
  await waitFor(
    () => childrenOf(spawned.pid).some((p) => commOf(p) === expectedComm()),
    20_000, "versioned payload exec'd",
  );
  const payload = childrenOf(spawned.pid).find((p) => commOf(p) === expectedComm())!;
  return { spawned, payload };
}

function spawnedJournal(jobID: string, pid: number): JobJournal {
  return journal.create({
    job_id: jobID, persona_id: "p-ver", runner_id: "ver-it",
    pid, start_ticks: startTicks(pid), boot_id: bootID(),
    unit_name: null, socket_path: journal.socketPath(jobID), stats_path: journal.statsPath(jobID),
    cgroup_mode: "prlimit", limits: {}, spec: { code_sha256: "x" }, usage_fact_id: `script:${jobID}:exec`,
  });
}

before(() => {
  // Missing binaries leave wd empty — every test below skips itself.
  if (!existsSync(RUNLIMITED_BIN) || !existsSync(WORKERD_SRC)) return;
  wd = mkdtempSync(join(tmpdir(), "sumi-ver-ident-"));
  journal = new Journal(wd);
  versioned = join(wd, "workerd-2026-08-04");
  symlinkSync(WORKERD_SRC, versioned);
  runner = new Runner({
    api: "http://127.0.0.1:1", token: "t", runnerID: "ver-it",
    workDir: join(wd, "runner"), workerdBin: versioned, runlimitedBin: RUNLIMITED_BIN,
    dispatcherPath: DISPATCHER,
    leaseMs: 1000, heartbeatMs: 100, claimLimit: 1, cgroupMode: "prlimit", log: () => {},
  });
});

after(async () => {
  for (const s of spawnedList) {
    try { s.killGroup(); } catch { /* gone */ }
    try { s.kill(); } catch { /* gone */ }
  }
  try { rmSync(wd, { recursive: true, force: true }); } catch { /* keep evidence */ }
});

test("identity: the payload is found by its derived image, never the literal name", { timeout: 60_000 }, async (t) => {
  if (!wd) return t.skip("no binaries");
  const { spawned, payload } = await spawnVersioned("j-ver-id");
  // The pre-fix predicate is demonstrably blind on this exact tree —
  // this is the production defect, not a mirrored assertion.
  assert.equal(findDescendant(spawned.pid, "workerd"), null,
    "literal 'workerd' must NOT match the versioned image");
  const workerdPid = (runner as unknown as { workerdPid?: (p: number) => number | null }).workerdPid;
  assert.ok(typeof workerdPid === "function",
    "runner must resolve the payload by configured image (pre-fix: method absent)");
  assert.equal(workerdPid!.call(runner, spawned.pid), payload,
    `derived image resolves the real payload pid (comm=${commOf(payload)})`);
});

test("ordered termination: payload dies first, wait4 stats survive the kill (F400)", { timeout: 60_000 }, async (t) => {
  if (!wd) return t.skip("no binaries");
  const { spawned, payload } = await spawnVersioned("j-ver-term");
  const j = spawnedJournal("j-ver-term", spawned.pid); // pre-resolve journal: pid = wrapper
  const terminate = (runner as unknown as {
    terminate?: (jj: JobJournal, s: Spawned, why: "wall_timeout", kill: boolean) => Promise<JournalExit | null>;
  }).terminate;
  assert.ok(typeof terminate === "function");
  const exit = await terminate!.call(runner, j, spawned, "wall_timeout", true);
  // The F400 signature: the literal-name walk missed the payload, the
  // group kill SIGKILLed runlimited mid-wait4, and usage came back
  // 'unknown'. The fix kills the payload by derived identity and lets
  // the wrapper record wait4 first.
  assert.equal(exit?.cpu_ms != null, true, `wait4 cpu_ms must be measured, got ${JSON.stringify(exit)}`);
  assert.equal(exit?.rusage_source, "wait4");
  assert.equal(exit?.signal, "SIGKILL");
  assert.equal(alive(payload), false, "payload dead");
  assert.equal(alive(spawned.pid), false, "wrapper dead");
  assert.equal(existsSync(journal.statsPath("j-ver-term")), true, "stats file written");
});

test("startup reconcile reaps a versioned orphan the literal name cannot see", { timeout: 60_000 }, async (t) => {
  if (!wd) return t.skip("no binaries");
  // Marker-less config path: killOrphanGroup's argv ownership proof
  // cannot fire, so this isolates name-vs-derived identity. Pre-fix the
  // literal walk misses the payload and the orphan survives; the fix
  // reaps it by configured image.
  const { spawned, payload } = await spawnVersioned("j-ver-reap", false);
  spawnedJournal("j-ver-reap", spawned.pid);
  const rec = new Reconciler({ client: stubClient, journal, runnerID: "ver-it", workerdBin: versioned, log: () => {} });
  await rec.run();
  await waitFor(() => !alive(payload), 5_000, "orphan payload reaped");
  assert.equal(alive(payload), false, "versioned payload reaped by derived identity");
  const st = journal.read("j-ver-reap")!;
  assert.ok(st.status === "reaped" || st.status === "reported", `journal ${st.status}`);
});

test("boundary: without the configured path the fallback is name-bound and cannot reap", { timeout: 60_000 }, async (t) => {
  if (!wd) return t.skip("no binaries");
  // workerdBin unset + no argv marker: neither the name walk nor the
  // group-kill proof can reach a versioned payload — the exact reason
  // production must wire workerdBin (main.ts). This pins the fallback
  // as compatibility-only, not a repair.
  const { spawned, payload } = await spawnVersioned("j-ver-fallback", false);
  spawnedJournal("j-ver-fallback", spawned.pid);
  const rec = new Reconciler({ client: stubClient, journal, runnerID: "ver-it", log: () => {} });
  await rec.run();
  await sleep(300);
  assert.equal(alive(payload), true,
    "unconfigured fallback must NOT kill what it cannot name — the honest limitation workerdBin removes");
  // Own cleanup: this orphan is ours and still runs.
  try { process.kill(payload, "SIGKILL"); } catch { /* gone */ }
});
