#!/usr/bin/env node
/**
 * production-resource-proof.mts — ROOT-RUN proof of the script-job
 * resource contract through the ACTUAL production path.
 *
 * What this proves (and how):
 *   Two real `Runner.runJob` executions run concurrently — job A is a
 *   ${MEMHOG_MIB}MiB allocator under `memory_mib=${MEMORY_MIB}` with a
 *   short real `wall_ms`, job B is a sibling sleeper. Production
 *   `spawnJob` launches each workerd inside its own
 *   `systemd-run --scope -p MemoryMax -p MemoryHigh -p MemorySwapMax=0
 *   -p TasksMax` scope; production `drive()` enforces the wall deadline
 *   and `terminate()` kills by verified identity.
 *
 *   The contract being proved is NOT "the kernel must oom_kill": with
 *   MemoryHigh throttling a hog can legitimately stall below max. The
 *   contract is (a) the job's memory footprint cannot escape the
 *   configured bound — verified LIVE from the scope's own cgroup while
 *   it exists — and (b) a resource-throttled job cannot run forever —
 *   the supervisor's wall deadline terminates it — while (c) a sibling
 *   job completes independently and (d) usage/result evidence is
 *   measured, not fabricated.
 *
 *   Acceptable terminations for A, distinguished in the result:
 *     - kernel stop: oom_kill>=1 in the scope's own memory.events while
 *       live (dispatch then errors → 'worker_error'), or
 *     - supervisor stop: complete() status 'failed' with
 *       result.reason 'wall_timeout' and measured usage.
 *   A 'done'/ok complete for A is a bound violation; a workload still
 *   alive after its deadline is a proof failure — never silently
 *   accepted.
 *
 *   Honest boundary: the ONLY stub is the state API (heartbeat →
 *   'running', complete/usage recorded, fileops empty). The claim/lease
 *   surface is owned elsewhere; everything under test — spawn, cgroup,
 *   journal, drive, terminate, usage — is production code.
 *
 * Required env: WORKERD_BIN, RUNLIMITED_BIN, DISPATCHER_JS, WORKDIR.
 * Optional env: MEMORY_MIB=256 MEMHOG_MIB=512 WALL_MS_A=30000
 *               SIBLING_MS=15000 SIBLING_MEM_MIB=128
 * Exit 0 = contract held. Exit 1 = a check failed. Exit 2 = env missing
 * or no systemd scope mechanism. Evidence: <WORKDIR>/evidence.json +
 * cgroup-samples.jsonl — never deleted.
 */
import { createServer, type Server } from "node:http";
import { execFileSync } from "node:child_process";
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Runner, defaultDispatcherPath } from "../src/runner.ts";
import type { JobRow } from "../src/api.ts";

const req = (k: string): string => {
  const v = process.env[k];
  if (!v) { console.error(`missing env ${k}`); process.exit(2); }
  return v;
};
const WORKERD_BIN = req("WORKERD_BIN");
const RUNLIMITED_BIN = req("RUNLIMITED_BIN");
const DISPATCHER_JS = process.env.DISPATCHER_JS ?? defaultDispatcherPath();
const WORKDIR = req("WORKDIR");
const MEMORY_MIB = Number(process.env.MEMORY_MIB ?? 256);
const MEMHOG_MIB = Number(process.env.MEMHOG_MIB ?? 512);
const WALL_MS_A = Number(process.env.WALL_MS_A ?? 30_000);
const SIBLING_MS = Number(process.env.SIBLING_MS ?? 15_000);
const SIBLING_MEM_MIB = Number(process.env.SIBLING_MEM_MIB ?? 128);
const TOKEN = "fixture-runtime-token";
const PERSONA = "fixture-persona";

mkdirSync(WORKDIR, { recursive: true });
const EVID = join(WORKDIR, "evidence.json");
const SAMPLES = join(WORKDIR, "cgroup-samples.jsonl");
writeFileSync(SAMPLES, "");

const checks: { name: string; pass: boolean; detail?: string }[] = [];
const ck = (name: string, pass: boolean, detail = "") => {
  checks.push({ name, pass, detail });
  console.log(`  ${pass ? "ok " : "FAIL"} ${name}${detail ? ` — ${detail}` : ""}`);
};

// ---------- owned stub state API (see header: only stub under test) --
const completions: Record<string, { status: string; result: Record<string, unknown>; error: string; at: number }> = {};
const usageFacts: Record<string, unknown>[] = [];
const stub: Server = createServer((rq, rs) => {
  const url = new URL(rq.url ?? "/", "http://stub");
  const m = url.pathname.match(/^\/internal\/core\/personas\/([^/]+)\/(.*)$/);
  let body = "";
  rq.on("data", (c) => (body += c));
  rq.on("end", () => {
    const json = (o: unknown, code = 200) => {
      rs.writeHead(code, { "content-type": "application/json" });
      rs.end(JSON.stringify(o));
    };
    if (rq.headers.authorization !== `Bearer ${TOKEN}`) return json({ error: "unauthorized" }, 401);
    if (!m) return json({ error: "not found" }, 404);
    const [, persona, rest] = m;
    const b = body ? JSON.parse(body) : {};
    let mm: RegExpMatchArray | null;
    if ((mm = rest.match(/^jobs\/([^/]+)\/heartbeat$/)) && rq.method === "POST") {
      return json({ job: { job_id: mm[1], persona_id: persona, kind: "script", status: "running", request: null, result: null, claimed_by: b.runner_id, claim_expires_at: null, error: null } });
    }
    if ((mm = rest.match(/^jobs\/([^/]+)\/complete$/)) && rq.method === "POST") {
      completions[mm[1]] = { status: b.status, result: b.result, error: b.error, at: Date.now() };
      return json({ job: { job_id: mm[1], status: b.status } });
    }
    if ((mm = rest.match(/^jobs\/([^/]+)\/files\/ops$/)) && rq.method === "GET") {
      return json({ ops: [], pending: 0 });
    }
    if ((mm = rest.match(/^jobs\/([^/]+)$/)) && rq.method === "GET") {
      return json({ job: { job_id: mm[1], persona_id: persona, status: "running" } });
    }
    if (rest === "usage/record" && rq.method === "POST") {
      usageFacts.push({ ...b, _at: Date.now() });
      return json({ ok: true });
    }
    if ((mm = rest.match(/^jobs\/([^/]+)\/lost-outcome$/)) && rq.method === "POST") {
      return json({ error: "route not wired — shared seam owned by copy-lotus" }, 404);
    }
    return json({ error: `stub: no route ${rq.method} ${url.pathname}` }, 404);
  });
});

// ---------- live cgroup observer for job A's scope -------------------
const unitA = "sumi-script-memhog-a.scope";
let aCgPath: string | null = null;
let aVerified = false;
let aPeak = 0;
let aMaxSwap = 0;
let aOomKill = 0;
let aHighEvents = 0;
let aBoundSeen = false;   // memory.max==cap AND swap.max==0 observed live
let aMemberPid: number | null = null;
const sample = () => {
  try {
    if (!aCgPath) {
      const cg = execFileSync("systemctl", ["--user", "show", unitA, "-p", "ControlGroup", "--value"], { encoding: "utf8" }).trim();
      if (cg) aCgPath = `/sys/fs/cgroup${cg}`;
    }
    if (!aCgPath) return;
    const rd = (f: string) => {
      try { return readFileSync(`${aCgPath}/${f}`, "utf8").trim(); }
      catch { return null; }
    };
    const cur = Number(rd("memory.current") ?? -1);
    const peak = Number(rd("memory.peak") ?? -1);
    const swap = Number(rd("memory.swap.current") ?? -1);
    const max = rd("memory.max");
    const swapmax = rd("memory.swap.max");
    const ev = rd("memory.events") ?? "";
    const get = (k: string) => Number((ev.match(new RegExp(`^${k} (\\d+)`, "m")) ?? [])[1] ?? 0);
    const procs = (rd("cgroup.procs") ?? "").split("\n").filter(Boolean).map(Number);
    if (!aVerified && aMemberPid != null && procs.includes(aMemberPid)) aVerified = true;
    if (Number(max) === MEMORY_MIB * 1048576 && swapmax === "0") aBoundSeen = true;
    aPeak = Math.max(aPeak, cur, peak);
    aMaxSwap = Math.max(aMaxSwap, swap);
    aOomKill = Math.max(aOomKill, get("oom_kill"));
    aHighEvents = Math.max(aHighEvents, get("high"));
    appendFileSync(SAMPLES, JSON.stringify({ t: Date.now(), cur, peak, swap, max, swapmax, high: get("high"), max_ev: get("max"), oom: get("oom"), oom_kill: get("oom_kill"), procs: procs.length }) + "\n");
  } catch { /* scope gone — sampling ends */ }
};

const job = (id: string, request: unknown): JobRow => ({
  job_id: id, persona_id: PERSONA, kind: "script", status: "running",
  request, result: null, claimed_by: "fixture-runner", claim_expires_at: null, error: null,
});
const limits = (over: Record<string, number>) => ({
  cpu_seconds: 120, wall_ms: 30_000, memory_mib: MEMORY_MIB,
  output_bytes: 64 << 10, log_bytes: 16 << 10, file_calls: 0, file_bytes: 0,
  ...over,
});

const MEMHOG = `export async function run(){ const c=[]; for(let i=0;i<${MEMHOG_MIB};i++) c.push(new Uint8Array(1048576).fill(1)); return c.length; }`;
const SLEEPER = `export async function run(){ await new Promise(r=>setTimeout(r,${SIBLING_MS})); return "slept"; }`;

const jobA = job("memhog-a", { code: MEMHOG, input: null, limits: limits({ wall_ms: WALL_MS_A }) });
const jobB = job("sibling-b", { code: SLEEPER, input: null, limits: limits({ wall_ms: 45_000, memory_mib: SIBLING_MEM_MIB, cpu_seconds: 30 }) });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const cleanup = () => {
  try { execFileSync("systemctl", ["--user", "stop", unitA], { stdio: "ignore" }); } catch { /* gone */ }
  try { execFileSync("systemctl", ["--user", "stop", "sumi-script-sibling-b.scope"], { stdio: "ignore" }); } catch { /* gone */ }
};
process.on("exit", cleanup);

await new Promise<void>((r) => stub.listen(0, "127.0.0.1", r));
const api = `http://127.0.0.1:${(stub.address() as { port: number }).port}`;

// Preflight: systemd scope mechanism must exist before claiming anything.
try {
  execFileSync("which", ["systemd-run"], { stdio: "ignore" });
  execFileSync("systemctl", ["--user", "is-system-running"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] });
} catch (e) {
  const out = String((e as { stdout?: string }).stdout ?? "");
  if (!/running|degraded/.test(out)) {
    console.error("exit2: no systemd user scope mechanism on this host");
    process.exit(2);
  }
}

const runner = new Runner({
  api, token: TOKEN, runnerID: "fixture-runner",
  workDir: join(WORKDIR, "journal"),
  workerdBin: WORKERD_BIN, runlimitedBin: RUNLIMITED_BIN,
  dispatcherPath: DISPATCHER_JS,
  leaseMs: 60_000, heartbeatMs: 1_000, claimLimit: 2,
  cgroupMode: "systemd",
  log: (l) => console.log(`  [runner] ${l}`),
});

console.log(`== production resource proof: A hog ${MEMHOG_MIB}MiB under ${MEMORY_MIB}MiB scope, wall ${WALL_MS_A}ms; B sleeps ${SIBLING_MS}ms ==`);

const sampler = setInterval(sample, 250);
const t0 = Date.now();
// Watch the journal for A's recorded workerd pid to verify membership.
const pidWatcher = setInterval(() => {
  if (aMemberPid != null) return;
  const j = runner.journal.read("memhog-a");
  if (j?.pid) aMemberPid = j.pid;
}, 100);

// Hard overall bound: never run past 4× the A deadline. The timeout
// races the jobs — a lost race means a job is still running, which the
// assertions then report honestly (leftovers/completions checked after).
await Promise.race([
  Promise.all([runner.runJob(jobA), runner.runJob(jobB)]),
  sleep(WALL_MS_A * 4 + 30_000).then(() => { throw new Error("fixture exceeded its hard runtime bound"); }),
]).catch((e) => { console.error(`fixture error: ${e}`); });
clearInterval(sampler);
clearInterval(pidWatcher);

// ---------- assertions ------------------------------------------------
const capBytes = MEMORY_MIB * 1048576;
const compA = completions["memhog-a"];
const compB = completions["sibling-b"];
const usageA = usageFacts.find((f) => (f.quantities as Record<string, unknown>)?.job_id === "memhog-a") as { quantities?: Record<string, unknown> } | undefined;

ck("live cgroup resolved for A's scope", aCgPath != null, aCgPath ?? "");
ck("workerd membership verified in scope cgroup.procs", aVerified, `pid=${aMemberPid}`);
ck("configured bound observed live: memory.max=cap AND swap.max=0", aBoundSeen,
  `cap=${MEMORY_MIB * 1048576} (a disappeared cgroup proves nothing — must be seen while live)`);
ck("footprint never escaped the cap (live peak ≤ memory.max)", aPeak > 0 && aPeak <= capBytes, `peak=${aPeak} cap=${capBytes}`);
ck("no swap usage under swap.max=0", aMaxSwap === 0, `swap.current max=${aMaxSwap}`);

// Termination: kernel stop OR supervisor wall stop — both prove the
// job cannot run forever; anything else fails.
const aReason = (compA?.result?.reason as string) ?? "";
const aUsage = (compA?.result?.usage ?? {}) as Record<string, unknown>;
const kernelStop = aOomKill >= 1;
const supervisorStop = compA?.status === "failed" && aReason === "wall_timeout";
ck("A terminated by kernel OOM or supervisor wall deadline", kernelStop || supervisorStop,
  `status=${compA?.status} reason=${aReason || "n/a"} oom_kill=${aOomKill} high_events=${aHighEvents} stop=${kernelStop ? "kernel" : supervisorStop ? "supervisor" : "none"}`);
ck("A did NOT return success (bound held or supervisor stopped it)",
  !(compA?.status === "done"), `status=${compA?.status}`);
ck("A usage measured, not fabricated", aUsage.cpu_ms_source === "measured" && aUsage.rusage_source === "wait4",
  `cpu_ms=${aUsage.cpu_ms} rss=${aUsage.max_rss_bytes} sig=${aUsage.exit_signal} enforcement=${aUsage.memory_enforcement}`);
ck("A usage carries the cgroup enforcement label", aUsage.memory_enforcement === "cgroup");

ck("sibling completed independently with exact value",
  compB?.status === "done" && (compB?.result?.value as unknown) === "slept",
  `status=${compB?.status} value=${JSON.stringify(compB?.result?.value)}`);
ck("sibling finished while A was still under the bound",
  compB != null && compA != null && (compB.at <= compA.at || kernelStop),
  `B@${compB?.at} A@${compA?.at} (ordering waived only if kernel stopped A first)`);
ck("usage fact recorded for A with real terminal status",
  usageA != null && (usageA.quantities?.terminal_status === "failed" || usageA.quantities?.terminal_status === "done" || usageA.quantities?.terminal_status === "cancelled"),
  `status=${usageA?.quantities?.terminal_status}`);

// Post: nothing owned survives. Match comm==workerd AND the fixture's
// own WORKDIR in argv — a bare substring pgrep matches the invoking
// shell's own cmdline and produces a false positive.
await sleep(500);
let leftovers = "";
try {
  leftovers = execFileSync("bash", ["-c",
    `ps -eo pid=,comm=,args= | awk -v w="${WORKDIR}" '$2=="workerd" && index($0,w) {print $1" "$3" "$4}'`,
  ], { encoding: "utf8" }).trim();
} catch { /* none */ }
ck("no owned workerd survives after the run", leftovers === "", leftovers.slice(0, 120));

const failed = checks.filter((c) => !c.pass);
writeFileSync(EVID, JSON.stringify({
  at: new Date().toISOString(), wall_ms: Date.now() - t0,
  contract: "total-footprint memory bound (MemoryMax+MemorySwapMax=0) + supervisor wall deadline",
  a: { stop: kernelStop ? "kernel_oom" : supervisorStop ? "supervisor_wall_timeout" : "none", oom_kill: aOomKill, high_events: aHighEvents, live_peak_bytes: aPeak, cap_bytes: capBytes, completion: compA },
  b: { completion: compB },
  usageFacts, checks,
}, null, 2));
stub.close();
console.log(`\nRESULT: ${checks.length - failed.length} passed, ${failed.length} failed — evidence: ${EVID} + ${SAMPLES}`);
process.exit(failed.length === 0 ? 0 : 1);
