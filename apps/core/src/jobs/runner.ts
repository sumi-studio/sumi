/**
 * Local job runner: a Node process, separate from the secretary, that owns
 * background job execution. It claims queued jobs from the state service,
 * spawns real local subprocesses, heartbeats its claim while they run, and
 * durably completes the record — which is also what queues the secretary's
 * notification input. The runner never takes the writer lease and holds no
 * secretary state; killing it at any point is safe — an expired claim is
 * swept to 'lost' (indeterminate), never silently re-executed.
 *
 * Truthful limits: this is a plain local subprocess, not a sandbox. The
 * command runs with this process's privileges; `cwd` is confined to the
 * workspace root and output is bounded, but there is no container, no
 * network isolation, and no resource limits beyond the timeout. External
 * effects are at-least-recorded, not exactly-once.
 */

import { spawn } from "node:child_process";
import path from "node:path";
import type { StateClient } from "../state-client.ts";
import { StateError } from "../state-client.ts";
import type { Job, JobTerminalReport, SubprocessJobRequest } from "../types.ts";

export interface JobRunnerConfig {
  personaId: string;
  /** Unique per runner process — the claim owner. */
  runnerId: string;
  state: StateClient;
  /** Base directory job `cwd` values resolve inside of. */
  workspaceRoot: string;
  /** Job kinds this runner executes. Default: ["subprocess"]. */
  kinds?: string[];
  /** Claim lease; heartbeats run at ~leaseMs/3. Default 30_000. */
  leaseMs?: number;
  /** Poll interval for claiming queued jobs. Default 1_000. */
  pollMs?: number;
  /** Max concurrent executions. Default 4. */
  concurrency?: number;
  /** Per-stream capture cap; excess is dropped and flagged. Default 256 KiB. */
  maxOutputBytes?: number;
  /** Default timeout when the request omits timeout_ms. Default 300_000. */
  defaultTimeoutMs?: number;
  /** Extra environment variables merged into every child. */
  baseEnv?: Record<string, string>;
  log?: (msg: string, fields?: Record<string, unknown>) => void;
  /** Spawn injection point for tests; defaults to a real child_process spawn. */
  spawnFn?: SpawnFn;
}

interface ChildHandle {
  pid: number | undefined;
  kill(signal?: NodeJS.Signals | number): boolean;
  stdout: NodeJS.ReadableStream | null;
  stderr: NodeJS.ReadableStream | null;
  on(event: "exit" | "error", cb: (...args: never[]) => void): unknown;
  once(event: "exit" | "error", cb: (...args: never[]) => void): unknown;
}

export type SpawnFn = (
  argv: string[],
  opts: { cwd: string; env: Record<string, string> },
) => ChildHandle;

const realSpawn: SpawnFn = (argv, opts) =>
  spawn(argv[0] as string, argv.slice(1), {
    cwd: opts.cwd,
    env: opts.env,
    shell: false,
  }) as unknown as ChildHandle;

interface Running {
  job: Job;
  child: ChildHandle | null;
  cancelObserved: boolean;
  timedOut: boolean;
  /** Runner is shutting down and killed the child itself. */
  runnerStopped: boolean;
  heartbeat: ReturnType<typeof setInterval> | null;
  timeout: ReturnType<typeof setTimeout> | null;
  stdout: string;
  stderr: string;
  stdoutTruncated: boolean;
  stderrTruncated: boolean;
  /** Set once completion has been durably recorded (or a conflict settled). */
  settled: boolean;
}

export class JobRunner {
  private readonly cfg: Required<
    Omit<JobRunnerConfig, "baseEnv" | "log" | "spawnFn">
  > &
    Pick<JobRunnerConfig, "baseEnv" | "log" | "spawnFn">;
  private readonly running = new Map<string, Running>();
  private stopped = false;
  private readonly spawnImpl: SpawnFn;
  private readonly log: (msg: string, fields?: Record<string, unknown>) => void;

  constructor(cfg: JobRunnerConfig) {
    this.cfg = {
      kinds: cfg.kinds ?? ["subprocess"],
      leaseMs: cfg.leaseMs ?? 30_000,
      pollMs: cfg.pollMs ?? 1_000,
      concurrency: cfg.concurrency ?? 4,
      maxOutputBytes: cfg.maxOutputBytes ?? 256 * 1024,
      defaultTimeoutMs: cfg.defaultTimeoutMs ?? 300_000,
      workspaceRoot: cfg.workspaceRoot,
      personaId: cfg.personaId,
      runnerId: cfg.runnerId,
      state: cfg.state,
      baseEnv: cfg.baseEnv,
      log: cfg.log,
      spawnFn: cfg.spawnFn,
    };
    this.spawnImpl = cfg.spawnFn ?? realSpawn;
    this.log = cfg.log ?? (() => {});
  }

  get activeCount(): number {
    return this.running.size;
  }

  /**
   * One claim pass: sweep expired claims (the service reconciles them to
   * 'lost' + notification), claim up to free capacity, and start each. Safe
   * to call repeatedly; a pass never blocks on a running job.
   */
  async step(): Promise<void> {
    if (this.stopped) return;
    const free = this.cfg.concurrency - this.running.size;
    if (free <= 0) return;
    const { claimed, swept } = await this.cfg.state.claimJobs(
      this.cfg.personaId,
      {
        runnerId: this.cfg.runnerId,
        kinds: this.cfg.kinds,
        leaseMs: this.cfg.leaseMs,
        limit: free,
      },
    );
    for (const j of swept) {
      this.log("job swept to lost (expired claim)", { job_id: j.job_id });
    }
    for (const job of claimed) {
      void this.execute(job).catch((e) => {
        this.log("job execution loop failed", {
          job_id: job.job_id,
          error: e instanceof Error ? e.message : String(e),
        });
      });
    }
  }

  /** Claim-poll loop until the signal aborts or stop() is called. */
  async run(signal?: AbortSignal): Promise<void> {
    while (!this.stopped && !signal?.aborted) {
      try {
        await this.step();
      } catch (e) {
        this.log("claim pass failed", {
          error: e instanceof Error ? e.message : String(e),
        });
      }
      await sleep(this.cfg.pollMs, signal);
    }
    await this.stop();
  }

  /**
   * Stop claiming and kill each live execution, recording the outcome the
   * runner deterministically knows: it stopped them (failed, "runner
   * stopped"). A hard kill -9 never reaches here — those claims expire and
   * the next claim pass (any runner) sweeps them to 'lost', the honest
   * record for an orphaned subprocess whose outcome is indeterminate.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    for (const r of this.running.values()) {
      r.runnerStopped = true;
      r.child?.kill("SIGKILL");
    }
    // Wait briefly for exit paths to record outcomes; if recording fails the
    // claim expires and reconciles to 'lost' anyway.
    const deadline = Date.now() + 5_000;
    while (this.running.size > 0 && Date.now() < deadline) {
      await sleep(50);
    }
    this.running.clear();
  }

  /** Resolve request.cwd inside the workspace root — no escaping it. */
  private resolveCwd(req: SubprocessJobRequest): string {
    const root = path.resolve(this.cfg.workspaceRoot);
    if (req.cwd === undefined || req.cwd === "") return root;
    if (path.isAbsolute(req.cwd)) {
      throw new Error("cwd must be relative to the workspace root");
    }
    const resolved = path.resolve(root, req.cwd);
    if (resolved !== root && !resolved.startsWith(root + path.sep)) {
      throw new Error("cwd escapes the workspace root");
    }
    return resolved;
  }

  private childEnv(req: SubprocessJobRequest): Record<string, string> {
    // A minimal, explicit environment — the child does not inherit this
    // process's full env (which may hold service tokens).
    const env: Record<string, string> = {
      PATH: process.env.PATH ?? "/usr/bin:/bin",
      HOME: process.env.HOME ?? "/",
      LANG: process.env.LANG ?? "C.UTF-8",
      ...this.cfg.baseEnv,
      ...(req.env ?? {}),
    };
    return env;
  }

  private clearTimers(r: Running) {
    if (r.heartbeat) clearInterval(r.heartbeat);
    if (r.timeout) clearTimeout(r.timeout);
    r.heartbeat = null;
    r.timeout = null;
  }

  private appendBounded(
    r: Running,
    stream: "stdout" | "stderr",
    chunk: Buffer,
  ) {
    const cur = stream === "stdout" ? r.stdout : r.stderr;
    const room = this.cfg.maxOutputBytes - Buffer.byteLength(cur);
    if (room <= 0) {
      if (stream === "stdout") r.stdoutTruncated = true;
      else r.stderrTruncated = true;
      return;
    }
    const take = chunk.subarray(0, room);
    if (take.length < chunk.length) {
      if (stream === "stdout") r.stdoutTruncated = true;
      else r.stderrTruncated = true;
    }
    if (stream === "stdout") r.stdout += take.toString("utf8");
    else r.stderr += take.toString("utf8");
  }

  /**
   * Run one claimed job to its durable terminal record. All failure paths
   * funnel to completeJob — the record is the deliverable. A job whose
   * completion response is lost retries the identical body; a conflict means
   * the stored verdict already stands and the local outcome is dropped.
   */
  private async execute(job: Job): Promise<void> {
    const r: Running = {
      job,
      child: null,
      cancelObserved: false,
      timedOut: false,
      runnerStopped: false,
      heartbeat: null,
      timeout: null,
      stdout: "",
      stderr: "",
      stdoutTruncated: false,
      stderrTruncated: false,
      settled: false,
    };
    this.running.set(job.job_id, r);
    try {
      const req = job.request as unknown as SubprocessJobRequest;
      let cwd: string;
      try {
        cwd = this.resolveCwd(req);
      } catch (e) {
        await this.finish(
          r,
          "failed",
          {},
          `request rejected: ${(e as Error).message}`,
        );
        return;
      }
      if (job.kind !== "subprocess") {
        await this.finish(r, "failed", {}, `unsupported job kind ${job.kind}`);
        return;
      }
      if (
        !Array.isArray(req.command) ||
        req.command.length === 0 ||
        req.command.some((a) => typeof a !== "string" || a === "")
      ) {
        await this.finish(
          r,
          "failed",
          {},
          "request rejected: command must be a non-empty string array",
        );
        return;
      }

      const child = this.spawnImpl(req.command, {
        cwd,
        env: this.childEnv(req),
      });
      r.child = child;
      child.stdout?.on("data", (c: Buffer) =>
        this.appendBounded(r, "stdout", c),
      );
      child.stderr?.on("data", (c: Buffer) =>
        this.appendBounded(r, "stderr", c),
      );

      const timeoutMs =
        typeof req.timeout_ms === "number"
          ? req.timeout_ms
          : this.cfg.defaultTimeoutMs;
      r.timeout = setTimeout(() => {
        r.timedOut = true;
        child.kill("SIGKILL");
      }, timeoutMs);

      r.heartbeat = setInterval(
        () => void this.beat(r),
        Math.max(200, Math.floor(this.cfg.leaseMs / 3)),
      );

      const { code, signal, spawnError } = await new Promise<{
        code: number | null;
        signal: NodeJS.Signals | null;
        spawnError: Error | null;
      }>((resolve) => {
        child.once("error", (err: Error) =>
          resolve({ code: null, signal: null, spawnError: err }),
        );
        child.once(
          "exit",
          (code: number | null, signal: NodeJS.Signals | null) =>
            resolve({ code, signal, spawnError: null }),
        );
      });
      this.clearTimers(r);
      if (r.settled) return; // completion already recorded (e.g. ownership loss)

      const result: Record<string, unknown> = {
        exit_code: code,
        stdout: r.stdout,
        stderr: r.stderr,
        stdout_truncated: r.stdoutTruncated,
        stderr_truncated: r.stderrTruncated,
      };
      if (spawnError) {
        result.spawn_error = spawnError.message;
        await this.finish(
          r,
          "failed",
          result,
          `spawn failed: ${spawnError.message}`,
        );
      } else if (r.runnerStopped) {
        await this.finish(
          r,
          "failed",
          result,
          "runner stopped; execution terminated before completion",
        );
      } else if (r.timedOut) {
        result.timed_out = true;
        await this.finish(r, "failed", result, `timeout after ${timeoutMs}ms`);
      } else if (r.cancelObserved || signal) {
        if (signal) result.signal = signal;
        await this.finish(
          r,
          "cancelled",
          result,
          r.cancelObserved ? "cancelled by request" : `killed by ${signal}`,
        );
      } else if (code === 0) {
        await this.finish(r, "done", result, "");
      } else {
        await this.finish(r, "failed", result, `exit code ${code}`);
      }
    } finally {
      this.clearTimers(r);
      this.running.delete(job.job_id);
    }
  }

  /**
   * Heartbeat: extends the claim and learns status. cancel_requested → stop
   * the process (the exit path then reports 'cancelled'). A 409 carrying a
   * terminal/lost stored row → ownership is gone; kill the child, mark
   * settled, and leave the honest record alone. Transient failures are
   * logged — the lease will expire and the sweep reconciles if we stay down.
   */
  private async beat(r: Running): Promise<void> {
    try {
      const job = await this.cfg.state.heartbeatJob(
        this.cfg.personaId,
        r.job.job_id,
        {
          runnerId: this.cfg.runnerId,
          leaseMs: this.cfg.leaseMs,
        },
      );
      if (job.status === "cancel_requested" && !r.cancelObserved) {
        r.cancelObserved = true;
        this.log("cancel observed", { job_id: r.job.job_id });
        r.child?.kill("SIGTERM");
        // Escalate if SIGTERM does not end it.
        setTimeout(() => r.child?.kill("SIGKILL"), 2_000);
      }
    } catch (e) {
      if (e instanceof StateError && e.status === 409) {
        // Terminal/lost/claimed-away: the durable record wins. Stop the
        // local execution; do not complete — the stored verdict stands.
        this.log("job ownership lost", {
          job_id: r.job.job_id,
          status: e.job?.status,
        });
        r.settled = true;
        r.child?.kill("SIGKILL");
        this.clearTimers(r);
        return;
      }
      this.log("heartbeat failed", {
        job_id: r.job.job_id,
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }

  /**
   * Record the terminal outcome. The response may be lost — retry the
   * identical body; identical replays return the stored row. A 409 means a
   * different verdict is already durable (e.g. the claim was swept to lost
   * or cancelled differently): accept it, log, stop retrying.
   */
  private async finish(
    r: Running,
    status: JobTerminalReport,
    result: Record<string, unknown>,
    error: string,
  ): Promise<void> {
    const attempts = 5;
    for (let i = 0; i < attempts && !r.settled; i++) {
      try {
        await this.cfg.state.completeJob(this.cfg.personaId, r.job.job_id, {
          runnerId: this.cfg.runnerId,
          status,
          result,
          error,
        });
        r.settled = true;
        this.log("job completed", { job_id: r.job.job_id, status });
        return;
      } catch (e) {
        if (e instanceof StateError && (e.status === 409 || e.status === 400)) {
          this.log("job completion settled by stored record", {
            job_id: r.job.job_id,
            stored: e.job?.status,
            error: e.message,
          });
          r.settled = true;
          return;
        }
        if (i + 1 < attempts) {
          await sleep(100 * 2 ** i);
        } else {
          this.log(
            "job completion could not be recorded (claim will expire → lost)",
            {
              job_id: r.job.job_id,
              status,
              error: e instanceof Error ? e.message : String(e),
            },
          );
        }
      }
    }
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(t);
        resolve();
      },
      { once: true },
    );
  });
}
