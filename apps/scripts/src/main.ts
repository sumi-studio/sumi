/**
 * Supervisor main: reconcile once, then loop — discover personas, claim
 * runnable script jobs, run each under its own supervised workerd.
 * Bounded global concurrency: at most `maxConcurrent` workers at once,
 * regardless of how many personas have queued jobs.
 */

import { Runner, defaultDispatcherPath, type RunnerConfig } from "./runner.ts";
import { Reconciler } from "./reconcile.ts";
import { ConfiguredDiscovery, SharedDiscovery, StateClient, type Discovery } from "./api.ts";
import { detectCgroupMode } from "./spawn.ts";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
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
}

export class Supervisor {
  readonly runner: Runner;
  private active = 0;
  private stop = false;

  readonly cfg: SupervisorConfig;
  constructor(cfg: SupervisorConfig) {
    this.cfg = cfg;
    this.runner = new Runner(cfg);
  }

  async start(): Promise<void> {
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
        // The claim IS the durable reservation — claiming beyond free
        // capacity would take ownership of jobs this process cannot
        // start, stranding them owned/running with no journal until the
        // lease sweeps them to 'lost'. Claim only what we can run now.
        const free = this.cfg.maxConcurrent - this.active;
        if (free <= 0) break;
        const claimed = await this.runner.claimPersona(p, Math.min(this.cfg.claimLimit, free));
        for (const job of claimed) {
          if (this.active >= this.cfg.maxConcurrent) break;
          this.active++;
          void this.runner.runJob(job).finally(() => { this.active--; });
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

  shutdown(): void {
    this.stop = true;
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
  const id = `scripts-${hostname()}-${randomBytes(8).toString("hex")}`;
  try {
    writeFileSync(f, id + "\n", { flag: "wx" });
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
    cgroupMode,
    log,
    discovery,
  });
}
