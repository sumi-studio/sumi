/**
 * Supervisor main: reconcile once, then loop — discover personas, claim
 * runnable script jobs, run each under its own supervised workerd.
 * Bounded global concurrency: at most `maxConcurrent` workers at once,
 * regardless of how many personas have queued jobs.
 */

import { Runner, defaultDispatcherPath, type RunnerConfig } from "./runner.ts";
import { Reconciler } from "./reconcile.ts";
import { ConfiguredDiscovery, type Discovery } from "./api.ts";
import { detectCgroupMode } from "./spawn.ts";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export interface SupervisorConfig extends RunnerConfig {
  discovery: Discovery;
  maxConcurrent: number;
  pollMs: number;
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
    this.cfg.log?.(`reconcile: reaped=${res.reaped} reported=${res.reported} failed=${res.failed}`);

    while (!this.stop) {
      let personas: string[] = [];
      try {
        personas = await this.cfg.discovery.personas();
      } catch (e) {
        this.cfg.log?.(`discovery: ${e}`);
      }
      for (const p of personas) {
        if (this.active >= this.cfg.maxConcurrent) break;
        const claimed = await this.runner.claimPersona(p);
        for (const job of claimed) {
          if (this.active >= this.cfg.maxConcurrent) break;
          this.active++;
          void this.runner.runJob(job).finally(() => { this.active--; });
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
  return new Supervisor({
    api: req("SUMI_STATE_API"),
    token: req("SUMI_STATE_TOKEN"),
    runnerID: process.env.SUMI_RUNNER_ID ?? `scripts-${process.pid}`,
    workDir: process.env.SUMI_WORK_DIR ?? `${process.env.HOME}/.local/state/sumi-scripts`,
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
    discovery: new ConfiguredDiscovery(personas),
  });
}
