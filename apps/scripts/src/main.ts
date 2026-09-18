/**
 * Supervisor main: reconcile once, then loop — discover personas, claim
 * runnable script jobs, run each under its own supervised workerd.
 * Bounded global concurrency: at most `maxConcurrent` workers at once,
 * regardless of how many personas have queued jobs.
 */

import { Runner, defaultDispatcherPath, type RunnerConfig } from "./runner.ts";
import { Reconciler } from "./reconcile.ts";
import { ConfiguredDiscovery, SharedDiscovery, StateClient, type Discovery, type JobRow } from "./api.ts";
import { detectCgroupMode } from "./spawn.ts";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { ensurePrivateDir } from "./privatefs.ts";
import { hostname } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export interface SupervisorConfig extends RunnerConfig {
  discovery: Discovery;
  maxConcurrent: number;
  pollMs: number;
  /** How often the recovery pass runs while the supervisor lives.
   *  Recovery delivers owed evidence only — it never touches this
   *  process's active executions. Default 30s. */
  recoveryEveryMs?: number;
  /** Bounded shutdown drain: how long shutdown() waits for in-flight
   *  jobs to report 'cancelled' before locally terminating the
   *  survivors. Default 20s. */
  shutdownGraceMs?: number;
  /** Bounded report-settle window after the deadline kill pass:
   *  killed children's drive loops get this long to unwind and land
   *  their honest reports before shutdown returns. Default 3s. */
  shutdownSettleMs?: number;
}

export class Supervisor {
  readonly runner: Runner;
  private active = 0;
  private inflight = new Map<string, JobRow>();
  /** Claims admitted during shutdown that are being reported as
   *  never-started — tracked so the drain waits for their honest
   *  terminal reports within the same bound. */
  private pendingReports = new Set<string>();
  /** Claim HTTP calls currently in flight — a resolving response is
   *  still this supervisor's pending work during shutdown. */
  private claimsInFlight = 0;
  private stop = false;
  private shutdownPromise: Promise<void> | null = null;

  readonly cfg: SupervisorConfig;
  constructor(cfg: SupervisorConfig) {
    this.cfg = cfg;
    this.runner = new Runner(cfg);
  }

  async start(): Promise<void> {
    // Before anything else: drop credential copies a dead run left
    // behind. No claim exists yet, so no in-flight worker can still be
    // reading its config — every run-*/config.capnp here is residue.
    const cleaned = this.runner.cleanStaleCredentialFiles();
    if (cleaned > 0) this.cfg.log?.(`startup: removed ${cleaned} stale config.capnp credential file(s)`);

    const rec = new Reconciler({
      client: this.runner.client,
      journal: this.runner.journal,
      runnerID: this.cfg.runnerID,
      log: this.cfg.log,
    });
    const res = await rec.run();
    this.cfg.log?.(`reconcile: reaped=${res.reaped} reported=${res.reported} failed=${res.failed} attention=${JSON.stringify(res.attention)}`);

    const everyMs = this.cfg.recoveryEveryMs ?? 30_000;
    let lastRecovery = Date.now();
    while (!this.stop) {
      let personas: string[] = [];
      try {
        personas = await this.cfg.discovery.personas();
      } catch (e) {
        this.cfg.log?.(`discovery: ${e}`);
      }
      for (const p of personas) {
        if (this.stop) break;
        // The claim IS the durable reservation — claiming beyond free
        // capacity would take ownership of jobs this process cannot
        // start, stranding them owned/running with no journal until the
        // lease sweeps them to 'lost'. Claim only what we can run now.
        const free = this.cfg.maxConcurrent - this.active;
        if (free <= 0) break;
        // The claim HTTP call is tracked: a response landing after stop
        // is still this supervisor's pending work — shutdown's drain
        // waits for it (bounded by the deadline), so every durable
        // reservation it admits is deliberately accounted for, never
        // silently abandoned.
        this.claimsInFlight++;
        let claimed: JobRow[];
        try {
          claimed = await this.runner.claimPersona(p, Math.min(this.cfg.claimLimit, free));
        } finally {
          this.claimsInFlight--;
        }
        for (const job of claimed) {
          // A claim request already in flight can resolve after stop:
          // the reservation is durable and ours, so it is accounted
          // honestly — reported 'cancelled'/shutdown_before_start
          // (provably never executed) rather than stranded until the
          // lease sweeps it to 'lost'.
          if (this.stop) {
            this.pendingReports.add(job.job_id);
            void this.runner.reportNeverStarted(job)
              .catch((e) => this.cfg.log?.(`shutdown: never-started report ${job.job_id}: ${e}`))
              .finally(() => this.pendingReports.delete(job.job_id));
            continue;
          }
          if (this.active >= this.cfg.maxConcurrent) break;
          this.active++;
          this.inflight.set(job.job_id, job);
          // Containment belt for the contained failure domain in
          // runJobInner: even if every inner layer failed to report, a
          // rejection here is logged, never an unhandledRejection that
          // kills the process and every sibling with it.
          void this.runner.runJob(job)
            .catch((e) => this.cfg.log?.(`job ${job.job_id} runJob failed (contained): ${e}`))
            .finally(() => { this.active--; this.inflight.delete(job.job_id); });
        }
      }
      // Periodic recovery: an API outage at startup, a lost Complete
      // reply, or an attention backlog deeper than one bounded pass all
      // get another opportunity WITHOUT a process restart — and the
      // pass is fenced to this supervisor's inactive work only.
      if (Date.now() - lastRecovery >= everyMs) {
        lastRecovery = Date.now();
        try {
          const r = await rec.runRecovery(this.runner.activeJobIds());
          if (r.reaped || r.reported || r.failed || r.attention.seen || r.attention.failed) {
            this.cfg.log?.(`recovery: reaped=${r.reaped} reported=${r.reported} failed=${r.failed} attention=${JSON.stringify(r.attention)}`);
          }
        } catch (e) {
          this.cfg.log?.(`recovery: ${e}`);
        }
      }
      await new Promise((r) => setTimeout(r, this.cfg.pollMs));
    }
  }

  /**
   * Graceful stop with a REAL wall-clock bound: total ≈ graceMs +
   * settleMs + a sub-second local kill pass.
   *
   *  1. Discovery/claims halt; the runner's pre-spawn guard refuses
   *     new launches. Claims still in flight that resolve are
   *     accounted honestly (reportNeverStarted — provably unexecuted).
   *  2. Cancels go to every in-flight job CONCURRENTLY — each bounded
   *     by the client's own per-request timeout and racing the drain.
   *     Sequential awaits would let N stalled requests spend N×timeout
   *     before the grace clock even started.
   *  3. Drain until the deadline: cancelled drive loops report
   *     'cancelled' with measured usage.
   *  4. Deadline reached: remaining children are killed LOCALLY —
   *     each detached spawn's owned process group (atomic across the
   *     fork→exec window a descendant walk cannot see) plus verified
   *     journal identities, never a broad kill. Children are NOT left
   *     to "their own rlimits": wall_ms is enforced by this process's
   *     drive loop which dies with us, and RLIMIT_CPU bounds CPU time,
   *     not an idle child's elapsed lifetime — an unsupervised sleeper
   *     would outlive us forever.
   *  5. Bounded settle window: killed jobs' drive loops unwind —
   *     terminate() collects wait4 stats, finish() attempts the
   *     honest report. Past this window we return anyway — journals
   *     hold frozen wire payloads, claims expire to 'lost', and the
   *     next supervisor's startup reconcile delivers the evidence
   *     idempotently.
   *
   * Idempotent — the same in-flight promise answers every caller.
   */
  shutdown(graceMs = this.cfg.shutdownGraceMs ?? 20_000): Promise<void> {
    this.shutdownPromise ??= this.doShutdown(graceMs);
    return this.shutdownPromise;
  }

  private pendingCount(): number {
    return this.inflight.size + this.pendingReports.size + this.claimsInFlight;
  }

  private async doShutdown(graceMs: number): Promise<void> {
    this.stop = true;
    this.runner.beginShutdown();
    const deadline = Date.now() + graceMs;
    for (const job of this.inflight.values()) {
      void this.runner.client.cancelJob(job.persona_id, job.job_id)
        .catch((e) => this.cfg.log?.(`shutdown: cancel ${job.job_id}: ${e}`));
    }
    while (this.pendingCount() > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, Math.min(100, Math.max(1, deadline - Date.now()))));
    }
    const remaining = [...this.inflight.keys()];
    for (const id of remaining) {
      try { this.runner.killOwned(id); } catch (e) { this.cfg.log?.(`shutdown: kill ${id}: ${e}`); }
    }
    if (remaining.length > 0) {
      this.cfg.log?.(`shutdown: ${remaining.length} job(s) locally terminated at deadline — drive loops report honest outcomes`);
    }
    const settleMs = this.cfg.shutdownSettleMs ?? 3_000;
    const settleDeadline = Date.now() + settleMs;
    while (this.pendingCount() > 0 && Date.now() < settleDeadline) {
      await new Promise((r) => setTimeout(r, Math.min(100, Math.max(1, settleDeadline - Date.now()))));
    }
    if (this.pendingCount() > 0) {
      this.cfg.log?.(`shutdown: ${this.pendingCount()} job(s) still reporting at exit — evidence journaled; claims resolve via lease expiry + reconcile`);
    }
  }
}

function defaultRunlimitedPath(): string {
  const here = dirname(fileURLToPath(import.meta.url));
  return join(here, "..", "bin", "runlimited");
}

/**
 * Runner identity must survive restarts: claims, the attention route
 * and lost-outcome attach are all keyed on runner_id — an identity that
 * cannot be recovered after restart orphans every in-flight job and
 * eats 403s on attach. Order: explicit SUMI_RUNNER_ID, else the durable
 * identity persisted under workDir, else a new one minted and stored.
 *
 * Claims must NEVER begin under an identity that cannot be recovered:
 * an existing-but-unreadable/empty runner-id file and any persistence
 * failure are startup errors, not reasons to mint an ephemeral id. The
 * minted id is random, not pid-derived — a recycled host PID must not
 * resurrect or collide with a prior identity. One supervisor per
 * workDir — two live processes sharing a runner_id is an unsupported
 * split-brain (claims, heartbeats and completes would fight).
 */
export function stableRunnerID(workDir: string): string {
  const env = process.env.SUMI_RUNNER_ID;
  if (env) return env;
  const f = join(workDir, "runner-id");
  let existing: string | null = null;
  try {
    existing = readFileSync(f, "utf8");
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code !== "ENOENT") {
      throw new Error(`runner-id ${f} exists but is unreadable — identity unrecoverable: ${e}`);
    }
  }
  if (existing != null) {
    const id = existing.trim();
    if (!id) {
      throw new Error(`runner-id ${f} exists but is empty — refusing to claim under an identity that cannot be recovered`);
    }
    return id;
  }
  mkdirSync(workDir, { recursive: true });
  ensurePrivateDir(workDir); // runner state is private from creation (F392)
  const id = `scripts-${hostname()}-${randomBytes(8).toString("hex")}`;
  try {
    writeFileSync(f, id + "\n", { flag: "wx", mode: 0o600 });
    return id;
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "EEXIST") {
      // A peer wrote it first — adopt the stored identity, but only if
      // it is actually recoverable.
      const cur = readFileSync(f, "utf8").trim();
      if (!cur) throw new Error(`runner-id ${f} exists but is empty — identity unrecoverable`);
      return cur;
    }
    throw new Error(`cannot persist runner identity in ${workDir} — refusing to claim under an identity that cannot be recovered: ${e}`);
  }
}

export function supervisorFromEnv(): Supervisor {
  const req = (name: string): string => {
    const v = process.env[name];
    if (!v) throw new Error(`${name} required`);
    return v;
  };
  const personas = (process.env.SUMI_PERSONAS ?? "")
    .split(",").map((s) => s.trim()).filter(Boolean);
  const modeEnv = process.env.SUMI_CGROUP_MODE;
  if (modeEnv != null && modeEnv !== "systemd" && modeEnv !== "prlimit") {
    throw new Error(`SUMI_CGROUP_MODE must be "systemd" or "prlimit", got ${JSON.stringify(modeEnv)}`);
  }
  // An explicit systemd request must fail honestly when the user manager
  // cannot launch scopes — silent degradation to the V8-heap-only bound
  // would let a memory hog through the configured limit unnoticed.
  const cgroupMode = modeEnv === "prlimit" ? "prlimit" : detectCgroupMode();
  if (modeEnv === "systemd" && cgroupMode !== "systemd") {
    throw new Error("SUMI_CGROUP_MODE=systemd but `systemd-run --user --scope` cannot launch in this environment");
  }
  const log = (l: string) => console.log(`[scripts] ${l}`);
  log(`cgroup mode: ${cgroupMode} (memory_enforcement: ${cgroupMode === "systemd" ? "cgroup MemoryMax/MemorySwapMax=0/TasksMax" : "V8 heap bound only — memory_mib is not a strict limit"})`);
  const api = req("SUMI_STATE_API");
  const token = req("SUMI_STATE_TOKEN");
  const workDir = process.env.SUMI_WORK_DIR ?? `${process.env.HOME}/.local/state/sumi-scripts`;
  const runnerID = stableRunnerID(workDir);
  // Discovery: the shared runnable route by default (no persona config
  // needed in production); SUMI_PERSONAS remains an explicit local/dev
  // filter — never a production requirement.
  const discovery: Discovery = personas.length > 0
    ? new ConfiguredDiscovery(personas)
    : new SharedDiscovery(new StateClient({ api, token }), ["script"],
        Number(process.env.SUMI_DISCOVERY_PAGE ?? 64));
  log(`runner_id: ${runnerID}; discovery: ${personas.length > 0 ? `static list (${personas.length} personas)` : "shared /internal/core/jobs/runnable"}`);
  return new Supervisor({
    api,
    token,
    runnerID,
    workDir,
    workerdBin: req("SUMI_WORKERD_BIN"),
    runlimitedBin: process.env.SUMI_RUNLIMITED_BIN ?? defaultRunlimitedPath(),
    dispatcherPath: process.env.SUMI_DISPATCHER_PATH ?? defaultDispatcherPath(),
    leaseMs: Number(process.env.SUMI_LEASE_MS ?? 30_000),
    heartbeatMs: Number(process.env.SUMI_HEARTBEAT_MS ?? 5_000),
    claimLimit: Number(process.env.SUMI_CLAIM_LIMIT ?? 4),
    maxConcurrent: Number(process.env.SUMI_MAX_CONCURRENT ?? 4),
    pollMs: Number(process.env.SUMI_POLL_MS ?? 1_000),
    shutdownGraceMs: Number(process.env.SUMI_SHUTDOWN_GRACE_MS ?? 20_000),
    shutdownSettleMs: Number(process.env.SUMI_SHUTDOWN_SETTLE_MS ?? 3_000),
    cgroupMode,
    log,
    discovery,
  });
}
