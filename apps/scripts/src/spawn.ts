/**
 * Per-job workerd launch. Two launch modes, selected at startup:
 *
 *  - "systemd": `systemd-run --user --scope` wraps the launcher in a
 *    transient cgroup v2 unit with MemoryMax, TasksMax, and
 *    MemorySwapMax=0 — per-job memory bounding and cgroup-wide
 *    termination. The bound contract is TOTAL footprint: memory_mib
 *    caps resident memory with no swap escape (root's host run proved
 *    an unbounded-swap scope lets a 2x hog return success under a
 *    MemoryMax it exceeded — swap traded termination for capacity,
 *    which is not the configured bound). Preferred on the deployment
 *    host, which has a user manager.
 *  - "prlimit": bare `runlimited` — a setrlimit+wait4 wrapper (built
 *    from src/runlimited.c) that applies RLIMIT_CPU to its exec'd child
 *    and reports wait4 rusage as JSON. Memory in this mode is bounded
 *    only by the V8 heap limit inside workerd (real but loose — the
 *    proof showed self-abort near ~1.48 GiB RSS regardless of the
 *    configured MiB). The runner reports memory_enforcement honestly.
 *
 * Wall-clock timeout and cancellation are always enforced by the
 * supervisor against the recorded process identity — independent of
 * which launcher produced the process.
 */

import { spawn as cpSpawn, execFileSync } from "node:child_process";
import { readFileSync, unlinkSync, existsSync } from "node:fs";

export interface SpawnInput {
  configPath: string;
  socketPath: string;
  statsPath: string;      // runlimited JSON rusage output
  cpuSeconds: number;
  memoryMib: number;
  cgroupMode: "systemd" | "prlimit";
  unitName?: string;
  workerdBin: string;
  runlimitedBin: string;  // path to the runlimited binary
  extraArgs?: string[];
}

/** Detect whether systemd --user scopes are launchable in this environment. */
export function detectCgroupMode(): "systemd" | "prlimit" {
  try {
    execFileSync("systemd-run", ["--user", "--scope", "--quiet", "--", "/bin/true"], {
      stdio: "ignore",
      timeout: 5000,
    });
    return "systemd";
  } catch {
    return "prlimit";
  }
}

export interface Spawned {
  pid: number;
  kill: () => void;
  wait: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

export function spawnJob(input: SpawnInput, onLog: (line: string) => void): Spawned {
  const inner = [
    input.runlimitedBin,
    `--cpu=${input.cpuSeconds}`,
    `--stats=${input.statsPath}`,
    "--",
    input.workerdBin, "serve", "--experimental", input.configPath,
    ...(input.extraArgs ?? []),
  ];
  const argv =
    input.cgroupMode === "systemd"
      ? [
          "systemd-run", "--user", "--scope", "--quiet",
          "--unit", input.unitName ?? `sumi-script-${process.pid}-${Date.now()}`,
          `--property=MemoryMax=${input.memoryMib}M`,
          `--property=MemoryHigh=${Math.max(16, input.memoryMib - 32)}M`,
          `--property=MemorySwapMax=0`,
          `--property=TasksMax=64`,
          "--",
          ...inner,
        ]
      : inner;

  try { unlinkSync(input.socketPath); } catch { /* absent */ }
  try { unlinkSync(input.statsPath); } catch { /* absent */ }

  const child = cpSpawn(argv[0]!, argv.slice(1), {
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stdout?.on("data", (d) => onLog(String(d).trimEnd()));
  child.stderr?.on("data", (d) => onLog(String(d).trimEnd()));

  const wait = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
    child.on("exit", (code, signal) => resolve({ code, signal }));
    // A launcher that cannot be spawned at all (ENOENT etc.) must not
    // crash the supervisor on an unhandled 'error' event — surface it
    // as a 127 exit so the job fails honestly instead.
    child.on("error", (e) => { onLog(`spawn error: ${e.message}`); resolve({ code: 127, signal: null }); });
  });

  return { pid: child.pid!, kill: () => { try { child.kill("SIGKILL"); } catch { /* gone */ } }, wait };
}

/** Parse the runlimited JSON rusage report. */
export function parseRusage(statsPath: string): { cpu_ms: number | null; max_rss_bytes: number | null; exit_code: number | null; signal: number | null } {
  if (!existsSync(statsPath)) return { cpu_ms: null, max_rss_bytes: null, exit_code: null, signal: null };
  try {
    const r = JSON.parse(readFileSync(statsPath, "utf8"));
    return {
      cpu_ms: typeof r.cpu_ms === "number" ? r.cpu_ms : null,
      max_rss_bytes: typeof r.max_rss_bytes === "number" ? r.max_rss_bytes : null,
      exit_code: typeof r.exit_code === "number" ? r.exit_code : null,
      signal: typeof r.signal === "number" ? r.signal : null,
    };
  } catch {
    return { cpu_ms: null, max_rss_bytes: null, exit_code: null, signal: null };
  }
}
