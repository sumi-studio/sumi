/**
 * Script job supervisor: claim → journal → spawn per-job workerd →
 * dispatch over a unix socket → monitor heartbeats/cancel/wall →
 * complete with honest usage evidence. One workerd process per job, so
 * a CPU-bound or memory-hogging script can never wedge a sibling job or
 * the control loop — the process is killed by identity, the claim is
 * never silently retried, and an indeterminate exit stays 'lost' or
 * 'failed', never re-executed.
 */

import { createHash } from "node:crypto";
import { mkdirSync, existsSync, unlinkSync, readdirSync } from "node:fs";
import { ensurePrivateDir } from "./privatefs.ts";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import * as http from "node:http";

import { StateClient, type JobRow } from "./api.ts";
import { Journal, type JobJournal, type JournalExit } from "./journal.ts";
import { normalizeSpec, SpecError, type ScriptSpec } from "./spec.ts";
import { writeConfig } from "./workerd/config.ts";
import { spawnJob, parseRusage, type Spawned } from "./spawn.ts";
import { bootID, startTicks, alive, findDescendant, killVerified, verifyIdentity } from "./proc.ts";

export interface RunnerConfig {
  api: string;
  token: string;            // internal runtime/admin credential
  runnerID: string;
  workDir: string;          // durable journal dir
  workerdBin: string;
  runlimitedBin: string;
  dispatcherPath: string;   // src/workerd/dispatcher.js
  leaseMs: number;
  heartbeatMs: number;
  claimLimit: number;
  cgroupMode: "systemd" | "prlimit";
  log?: (line: string) => void;
}

const TERMINAL = new Set(["done", "failed", "cancelled", "lost"]);

export function defaultDispatcherPath(): string {
  const here = dirname(fileURLToPath(import.meta.url));
  return join(here, "workerd", "dispatcher.js");
}

interface RunOutcome {
  status: "done" | "failed" | "cancelled";
  result: Record<string, unknown>;
  error: string;
}

export class Runner {
  readonly client: StateClient;
  readonly journal: Journal;
  private log: (line: string) => void;
  /** Jobs this live supervisor owns right now — recovery must never
   *  treat their journals as restart evidence (no reap, no re-report). */
  private activeJobs = new Set<string>();
  /** Live spawn handles keyed by job — the shutdown path's only way to
   *  reach a child. Registration is synchronous with spawnJob, so a
   *  child can never exist unregistered. */
  private liveSpawns = new Map<string, { spawned: Spawned; j: JobJournal }>();
  /** Set by beginShutdown(): the pre-spawn guard refuses new launches.
   *  spawn→register is synchronous, so once set no new child can appear
   *  after a kill pass. */
  private stopping = false;

  readonly cfg: RunnerConfig;
  constructor(cfg: RunnerConfig) {
    this.cfg = cfg;
    this.client = new StateClient({ api: cfg.api, token: cfg.token });
    this.journal = new Journal(cfg.workDir);
    this.log = cfg.log ?? ((l) => console.log(`[scripts] ${l}`));
    // Parents may not exist yet — recursive-create them, then pin OUR
    // dir to 0700 (only ours: parents are never chmod'd — F392).
    mkdirSync(cfg.workDir, { recursive: true });
    ensurePrivateDir(cfg.workDir);
  }

  /** Claim up to `limit` script jobs for one persona (default
   *  cfg.claimLimit). The caller bounds the reservation: a claim is
   *  durable immediately, so claiming more than we can start strands
   *  owned/running rows until their lease expires into 'lost'. */
  async claimPersona(personaID: string, limit = this.cfg.claimLimit): Promise<JobRow[]> {
    try {
      const { claimed, swept } = await this.client.claimJobs(
        personaID, this.cfg.runnerID, this.cfg.leaseMs, limit,
      );
      for (const j of swept) {
        this.log(`swept ${j.job_id} (expired claim -> ${j.status})`);
      }
      return claimed;
    } catch (e) {
      this.log(`claim ${personaID}: ${e}`);
      return [];
    }
  }

  /** Snapshot of job ids this supervisor currently owns. */
  activeJobIds(): ReadonlySet<string> {
    return this.activeJobs;
  }

  /** Execute one claimed job end to end. */
  async runJob(job: JobRow): Promise<void> {
    this.activeJobs.add(job.job_id);
    try {
      await this.runJobInner(job);
    } finally {
      this.activeJobs.delete(job.job_id);
    }
  }

  private async runJobInner(job: JobRow): Promise<void> {
    let spec: ScriptSpec;
    try {
      spec = normalizeSpec(job.request);
    } catch (e) {
      if (e instanceof SpecError) {
        // Bad stored request: fail terminally, never spawn.
        await this.finish(job, "failed", { reason: "invalid_spec", detail: e.message }, e.message);
        return;
      }
      throw e;
    }

    // The whole preparation + launch section is ONE contained failure
    // domain: mkdir, journal, config, spawn and the pid write can each
    // fault transiently (ENOSPC/EEXIST/EROFS, a missing dispatcher, a
    // degraded journal dir). A rejection here would escape to the
    // supervisor's fire-and-forget call as an unhandledRejection and
    // kill every sibling — so any throw becomes this job's honest
    // 'failed', never a process exit.
    let j: JobJournal | null = null;
    let spawned: Spawned | null = null;
    const jobDir = join(this.cfg.workDir, "tmp", `run-${job.job_id.replace(/[^A-Za-z0-9._:-]/g, "_")}`);
    // The credential file's path is deterministic — computed BEFORE
    // writeConfig so even a partial write leaves an owned, named file
    // the finally below can remove (F390: a throwing writeConfig used
    // to leave configPath null and the residue untracked).
    const configFile = join(jobDir, "config.capnp");
    try {
      // 0700 from creation / tightened when pre-existing-and-owned:
      // the job dir holds config.capnp (runtime token), the control
      // socket and rusage stats (F392).
      ensurePrivateDir(jobDir);
      const socketPath = this.journal.socketPath(job.job_id);
      const statsPath = this.journal.statsPath(job.job_id);
      const usageFactID = `script:${job.job_id}:exec`;

      j = this.journal.create({
        job_id: job.job_id,
        persona_id: job.persona_id,
        runner_id: this.cfg.runnerID,
        pid: null,
        start_ticks: null,
        boot_id: bootID(),
        unit_name: null,
        socket_path: socketPath,
        stats_path: statsPath,
        cgroup_mode: this.cfg.cgroupMode,
        limits: spec.limits as unknown as Record<string, number>,
        spec: { code_sha256: createHash("sha256").update(spec.code).digest("hex") },
        usage_fact_id: usageFactID,
      });

      writeConfig(jobDir, {
        socketPath,
        dispatcherPath: this.cfg.dispatcherPath,
        api: this.cfg.api,
        token: this.cfg.token,
        personaID: job.persona_id,
        jobID: job.job_id,
        runnerID: this.cfg.runnerID,
        limits: {
          cpu_ms: spec.limits.cpu_seconds * 1000,
          file_calls: spec.limits.file_calls,
          file_bytes: spec.limits.file_bytes,
          log_bytes: spec.limits.log_bytes,
        },
      });

      // Shutdown began between claim and launch — the script provably
      // never executed (spawn→register is one synchronous block, so
      // this check cannot be bypassed by a mid-launch stop). Report the
      // honest never-started outcome rather than stranding the claim
      // for lease expiry.
      if (this.stopping) {
        this.journal.update(j, { status: "exited" });
        await this.finish(job, "cancelled", {
          reason: "shutdown_before_start",
          detail: "claim admitted as supervisor shutdown began — script provably never executed",
        }, "shutdown before start");
        return;
      }

      const unitName = this.cfg.cgroupMode === "systemd"
        ? `sumi-script-${job.job_id.replace(/[^A-Za-z0-9]/g, "-")}`
        : undefined;

      spawned = spawnJob({
        configPath: configFile, socketPath, statsPath,
        cpuSeconds: spec.limits.cpu_seconds,
        memoryMib: spec.limits.memory_mib,
        cgroupMode: this.cfg.cgroupMode,
        unitName, workerdBin: this.cfg.workerdBin,
        runlimitedBin: this.cfg.runlimitedBin,
      }, (line) => this.log(`job ${job.job_id} workerd: ${line}`));
      this.liveSpawns.set(job.job_id, { spawned, j });

      // Record the leader's exact identity (pid + start_ticks): for a
      // detached spawn pgid == pid, and the orphan reaper needs the
      // leader's ticks for fork-order verification (F397).
      this.journal.update(j, {
        status: "spawned",
        spawned_at: new Date().toISOString(),
        pid: spawned.pid,
        start_ticks: startTicks(spawned.pid),
      });

      try {
        // Wait for the worker to accept on its unix socket — by then
        // workerd has read config.capnp once at startup, so the copy
        // holding the runtime token is no longer needed by anyone.
        await this.waitReady(socketPath, spawned, 30_000);
        this.unlinkConfig(configFile, job.job_id);
        // The spawned pid is the wrapper (systemd-run or /usr/bin/time);
        // workerd is its descendant — prlimit execs it, so it is the only
        // child in prlimit mode. Record the real workerd pid + start ticks
        // as the durable identity.
        const resolved = findDescendant(spawned.pid, "workerd") ?? spawned.pid;
        const ticks = startTicks(resolved);
        this.journal.update(j, { status: "running", pid: resolved, start_ticks: ticks });
        this.log(`job ${job.job_id} workerd ready pid=${resolved}`);

        const outcome = await this.drive(job, spec, j, socketPath, spawned);
        await this.finish(job, outcome.status, outcome.result, outcome.error);
      } catch (e) {
        // Dispatch/ready failure: kill the process by identity, capture
        // whatever rusage exists, and report failed (not re-executed).
        const exit = await this.terminate(j, spawned, "unknown");
        const result: Record<string, unknown> = {
          reason: "runner_error", detail: String(e),
          usage: this.usageFromExit(exit, spec),
        };
        await this.finish(job, "failed", result, String(e));
      }
    } catch (e) {
      // Preparation/launch fault. Whatever spawnJob managed to return is
      // killed by the handle still held — a child that exists without a
      // persisted pid must not outlive its bookkeeping. Report only what
      // is provable: 'failed'/runner_error, no execution claim and no
      // usage beyond what terminate could measure. If even the report
      // fails (degraded storage AND API), the claim still resolves
      // honestly by lease expiry into the attention path.
      this.log(`job ${job.job_id} preparation failed: ${e}`);
      if (spawned && j) {
        try { await this.terminate(j, spawned, "unknown"); } catch { /* nothing more provable */ }
      } else if (spawned) {
        // No journal to consult — kill by the held handle. A workerd
        // descendant gets a verified kill; anything still inside the
        // fork→exec window is unnameable to a walk, so the owned
        // process group is the atomic backstop (the j-pp leak).
        try {
          const workerPid = findDescendant(spawned.pid, "workerd");
          const killed = workerPid != null &&
            killVerified({ pid: workerPid, start_ticks: startTicks(workerPid), boot_id: bootID() }, "SIGKILL");
          if (!killed) spawned.killGroup();
          spawned.kill();
        } catch { /* best effort — bounded by its own rlimits */ }
      }
      try {
        await this.finish(job, "failed", { reason: "runner_error", detail: `preparation failed: ${String(e)}` }, String(e));
      } catch (e2) {
        this.log(`job ${job.job_id} failure report failed too: ${e2} — claim resolves by lease expiry`);
      }
    } finally {
      // The launch credential leaves on EVERY path: success (the
      // post-bind unlink already ran — ENOENT is ignored), ready
      // failure, spawn/pid-persist fault, partial write, cancel, or a
      // prep throw. Journals, rusage stats and run dirs stay — only
      // the token-bearing file is removed (F390).
      this.unlinkConfig(configFile, job.job_id);
      if (spawned) this.liveSpawns.delete(job.job_id);
    }
  }

  /** Shutdown has begun: the pre-spawn guard in runJobInner refuses new
   *  launches. spawn→register is synchronous, so once this is set no
   *  new child can appear after a kill pass. Idempotent. */
  beginShutdown(): void {
    this.stopping = true;
  }

  /**
   * Kill one owned child — synchronous, local-only, verified. The
   * bounded shutdown path's deadline enforcement and the emergency
   * exit path's only tool: SIGKILL the workerd by journal identity or
   * verified descendant, the owned process group as the exec-window
   * backstop, then the held wrapper handle. Never a broad kill, never
   * a guessed pid — the liveSpawns entry exists only because this
   * process spawned it. A child killed here unwinds its own drive
   * loop, which reports whatever it observed; if the process exits
   * first, the durable journal + claim expiry + reconcile carry the
   * evidence.
   */
  killOwned(jobID: string): void {
    const live = this.liveSpawns.get(jobID);
    if (!live) return;
    const { spawned, j } = live;
    const boot = j.boot_id;
    const pid = j.pid ?? spawned.pid;
    // Verified payload kill first — a dead wrapper orphans its child to
    // init, and the child still mid-exec (comm not yet "workerd") is
    // invisible to any descendant walk (the j-pp leak proved both).
    const workerPid = findDescendant(spawned.pid, "workerd");
    let payloadKilled = false;
    if (workerPid != null && workerPid !== pid) {
      payloadKilled = killVerified({ pid: workerPid, start_ticks: startTicks(workerPid), boot_id: boot }, "SIGKILL");
    }
    if (pid !== spawned.pid && verifyIdentity({ pid, start_ticks: j.start_ticks, boot_id: boot })) {
      payloadKilled = killVerified({ pid, start_ticks: j.start_ticks, boot_id: boot }, "SIGKILL") || payloadKilled;
    }
    // No payload could be verified — it is inside the fork→exec window
    // or never bound. The process group is atomic across that window:
    // nothing it will exec can escape.
    if (!payloadKilled) spawned.killGroup();
    if (alive(spawned.pid)) spawned.kill();
  }

  /** Emergency exit path: synchronously kill every child this process
   *  owns — held spawn handles, their owned process groups, and
   *  verified identities only. */
  killAllOwned(): void {
    for (const id of [...this.liveSpawns.keys()]) this.killOwned(id);
  }

  /**
   * A claim admitted but never launched — no spawn ever happened for
   * it, so "never executed" is provable locally (nothing else launches
   * claimed jobs). Report the honest terminal outcome directly instead
   * of stranding the reservation until lease expiry sweeps it to
   * 'lost'. Goes through finish() like every other outcome: ledger
   * settle, frozen wire payload, idempotent delivery.
   */
  async reportNeverStarted(job: JobRow): Promise<void> {
    await this.finish(job, "cancelled", {
      reason: "shutdown_before_start",
      detail: "claim admitted as supervisor shutdown began — script provably never executed",
    }, "shutdown before start");
  }

  /**
   * workerd reads config.capnp once at startup; the file carries the
   * cross-persona runtime token, so it is removed as soon as the worker
   * is bound — and on every failure/cancel path. Best-effort: an
   * unlink error is logged (path only, never file contents) and never
   * fails the job.
   */
  private unlinkConfig(path: string | null, jobID: string): void {
    if (!path) return;
    try {
      unlinkSync(path);
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code !== "ENOENT") {
        this.log(`job ${jobID}: config unlink failed: ${e}`);
      }
    }
  }

  /**
   * Remove config.capnp copies left behind by a dead supervisor run —
   * each holds a launch-time copy of the runtime token that no live
   * worker needs anymore. Journals, rusage stats and job dirs are kept;
   * only the credential file is removed. Safe to call at startup only,
   * before any claim: this process has no in-flight launches whose
   * worker might still be reading its config. Best-effort per file.
   */
  cleanStaleCredentialFiles(): number {
    const tmp = join(this.cfg.workDir, "tmp");
    let names: string[];
    try {
      names = readdirSync(tmp);
    } catch {
      return 0;
    }
    let removed = 0;
    for (const name of names) {
      if (!name.startsWith("run-")) continue;
      try {
        unlinkSync(join(tmp, name, "config.capnp"));
        removed++;
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code !== "ENOENT") {
          this.log(`stale config cleanup ${name}: ${e}`);
        }
      }
    }
    return removed;
  }

  /** The monitor loop: heartbeat, cancel observation, wall timeout. */
  private async drive(job: JobRow, spec: ScriptSpec, j: JobJournal, socketPath: string, spawned: Spawned): Promise<RunOutcome> {
    const deadline = Date.now() + spec.limits.wall_ms;
    const dispatch = this.postRun(socketPath, { code: spec.code, input: spec.input });
    let cancelled = false;

    for (;;) {
      const left = deadline - Date.now();
      if (left <= 0) {
        const exit = await this.terminate(j, spawned, "wall_timeout");
        const res = await dispatch.catch(() => null);
        return {
          status: "failed",
          result: {
            reason: "wall_timeout",
            limit_ms: spec.limits.wall_ms,
            logs: res?.logs ?? null,
            usage: this.usageFromExit(exit, spec),
          },
          error: `wall timeout ${spec.limits.wall_ms}ms`,
        };
      }

      const raced = await Promise.race([
        dispatch.then((r) => ({ kind: "done" as const, r })).catch((e) => ({ kind: "error" as const, e })),
        this.sleep(Math.min(this.cfg.heartbeatMs, left)).then(() => ({ kind: "tick" as const })),
      ]);

      if (raced.kind === "done" || raced.kind === "error") {
        // The run resolved — the worker has nothing left to do; reap it
        // (kill=false only when it already exited) so a completed job
        // never leaves an idle workerd behind.
        const exit = await this.terminate(j, spawned, "unknown", true);
        if (raced.kind === "error") {
          // The socket request failed — worker died or reset. If the exit
          // was SIGXCPU it was our CPU cap; otherwise a crash/kill.
          const reason = exit?.signal === "SIGXCPU" || exit?.signal === "SIGCPU" ? "cpu_limit" : "worker_error";
          return {
            status: "failed",
            result: { reason, detail: String(raced.e), usage: this.usageFromExit(exit, spec) },
            error: String(raced.e),
          };
        }
        const r = raced.r as Record<string, unknown>;
        const value = r.value;
        const ok = r.ok === true;
        const bounded = this.boundOutput(value, spec);
        return {
          status: ok ? (cancelled ? "cancelled" : "done") : "failed",
          result: {
            value: ok ? bounded.value : undefined,
            error: ok ? undefined : r.error,
            logs: r.logs ?? null,
            output_truncated: bounded.truncated || undefined,
            wall_ms: r.wall_ms,
            usage: this.usageFromExit(exit, spec),
          },
          error: ok ? "" : JSON.stringify(r.error ?? "script error"),
        };
      }

      // tick: heartbeat + cancel observation + liveness
      try {
        const { job: cur } = await this.client.heartbeat(
          job.persona_id, job.job_id, this.cfg.runnerID, this.cfg.leaseMs,
        );
        if (cur.status === "cancel_requested" && !cancelled) {
          cancelled = true;
          this.log(`job ${job.job_id} cancel requested`);
          const exit = await this.terminate(j, spawned, "cancel");
          return {
            status: "cancelled",
            result: { reason: "cancel_requested", usage: this.usageFromExit(exit, spec) },
            error: "cancelled",
          };
        }
        if (TERMINAL.has(cur.status)) {
          // The row left us (claimed away / swept / completed elsewhere):
          // stop the execution we still hold, do not report.
          await this.terminate(j, spawned, "unknown");
          return { status: "failed", result: { reason: "claim_lost" }, error: `job became ${cur.status}` };
        }
      } catch (e) {
        // Heartbeat failure (409 => claim lost; network => keep watching
        // until wall deadline; the claim expiry sweeps us to lost).
        const status = (e as { status?: number }).status;
        if (status === 409) {
          await this.terminate(j, spawned, "unknown");
          return { status: "failed", result: { reason: "claim_lost", detail: String(e) }, error: String(e) };
        }
        this.log(`job ${job.job_id} heartbeat: ${e}`);
      }
      if (j.pid != null && !alive(j.pid) && !spawnedExited(spawned)) {
        // Process vanished without the dispatch resolving — handled next
        // lap when the socket errors; keep it simple.
      }
    }
  }

  private async postRun(socketPath: string, body: unknown): Promise<Record<string, unknown>> {
    const payload = JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = http.request(
        { socketPath, path: "/run", method: "POST", headers: { "content-type": "application/json", "content-length": Buffer.byteLength(payload) } },
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
      req.write(payload);
      req.end();
    });
  }

  private boundOutput(value: unknown, spec: ScriptSpec): { value: unknown; truncated: boolean } {
    const raw = JSON.stringify(value ?? null) ?? "null";
    if (Buffer.byteLength(raw) <= spec.limits.output_bytes) {
      return { value, truncated: false };
    }
    return { value: raw.slice(0, spec.limits.output_bytes), truncated: true };
  }

  private usageFromExit(exit: JournalExit | null, spec: ScriptSpec): Record<string, unknown> {
    return {
      cpu_ms: exit?.cpu_ms ?? null,
      cpu_ms_source: exit?.cpu_ms != null ? "measured" : "unknown",
      max_rss_bytes: exit?.max_rss_bytes ?? null,
      max_rss_source: exit?.max_rss_bytes != null ? "measured" : "unknown",
      wall_ms: exit?.wall_ms ?? null,
      wall_ms_source: exit?.wall_ms != null ? "measured" : "unknown",
      cpu_limit_seconds: spec.limits.cpu_seconds,      // RLIMIT_CPU granularity
      memory_limit_mib: spec.limits.memory_mib,
      memory_enforcement: this.cfg.cgroupMode === "systemd" ? "cgroup" : "v8_heap_bound",
      exit_code: exit?.code ?? null,
      exit_signal: exit?.signal ?? null,
      rusage_source: exit?.rusage_source ?? "none",
    };
  }

  /** Kill the tracked workerd by identity (or just reap its exit status). */
  private async terminate(j: JobJournal, spawned: Spawned, why: JournalExit["killed_by"], kill = true): Promise<JournalExit | null> {
    const started = j.spawned_at ? Date.parse(j.spawned_at) : Date.now();
    let exit: JournalExit | null = null;
    try {
      const identity = { pid: j.pid ?? spawned.pid, start_ticks: j.start_ticks, boot_id: j.boot_id };
      if (kill) {
        // The workerd payload dies BEFORE the wrapper: killing the
        // wrapper first orphans the child to init, and every later
        // findDescendant(spawned.pid) walk then sees a dead process —
        // the orphan escapes (the j-pp leak proved this).
        const workerPid = findDescendant(spawned.pid, "workerd");
        let payloadKilled = false;
        if (workerPid != null && workerPid !== j.pid) {
          payloadKilled = killVerified({ pid: workerPid, start_ticks: startTicks(workerPid), boot_id: j.boot_id }, "SIGKILL");
        }
        // The recorded pid dies too — but only when it is NOT the
        // wrapper itself: runlimited gets a bounded window to record
        // the child's wait4 stats before it is killed — killing it
        // first is the race that loses measured usage.
        if (j.pid != null && j.pid !== spawned.pid && verifyIdentity(identity)) {
          payloadKilled = killVerified(identity, "SIGKILL") || payloadKilled;
        }
        if (!payloadKilled) {
          // Exec-window race: the payload is either not forked yet or
          // still mid-exec answering to comm "runlimited" — no walk can
          // name it. Killing only the wrapper would orphan a child that
          // execs workerd a moment later (exactly the j-pp leak). The
          // detached spawn's process group is atomic and owned: every
          // future descendant dies with it. Stats are honestly lost
          // here — no worker ever proved it bound — usage reports
          // 'unknown', not fabricated numbers.
          spawned.killGroup();
        }
        if (j.stats_path) {
          const deadline = Date.now() + 3000;
          while (!existsSync(j.stats_path) && Date.now() < deadline) {
            await this.sleep(100);
          }
        }
        if (alive(spawned.pid)) {
          spawned.kill();
        }
      }
      const res = await Promise.race([spawned.wait, this.sleep(5000).then(() => null)]);
      const rusage = parseRusage(j.stats_path ?? "");
      // The runlimited stats file carries the WORKER's exit status — the
      // spawned wrapper's own status is only a fallback (e.g. stats lost).
      const sigMap: Record<number, NodeJS.Signals> = { 9: "SIGKILL", 15: "SIGTERM", 24: "SIGXCPU", 6: "SIGABRT", 11: "SIGSEGV" };
      exit = {
        code: rusage.exit_code ?? res?.code ?? null,
        signal: (rusage.signal != null && rusage.signal !== 0
          ? (sigMap[rusage.signal] ?? `SIG${rusage.signal}` as NodeJS.Signals)
          : res?.signal) ?? null,
        wall_ms: Date.now() - started,
        cpu_ms: rusage.cpu_ms,
        max_rss_bytes: rusage.max_rss_bytes,
        rusage_source: rusage.cpu_ms != null ? "wait4" : "none",
        killed_by: why,
      };
    } catch { /* best effort */ }
    this.journal.update(j, {
      status: "exited", exited_at: new Date().toISOString(), exit,
    });
    return exit;
  }

  /** Resolve pending file ops, then CompleteJob; record usage facts. */
  private async finish(job: JobRow, status: string, result: Record<string, unknown>, error: string): Promise<void> {
    // Settle admitted file ops whose upstream call never resolved —
    // keyed resend through the API's resolve route, honest statuses.
    let pending: number | null = null;
    try {
      const { ops } = await this.client.listFileOps(job.persona_id, job.job_id, true);
      for (const op of ops) {
        try {
          await this.client.resolveFileOp(job.persona_id, job.job_id, op.op_id, this.cfg.runnerID);
        } catch { /* stays pending — reported below */ }
      }
      // Re-read the truth: a resolve call can return 200 while the op is
      // still 'unknown' (another lost upstream response). The terminal
      // record reports what the ledger actually says.
      const after = await this.client.listFileOps(job.persona_id, job.job_id, true);
      pending = after.pending;
    } catch { pending = null; }
    result.file_ops_pending = pending;

    // Persist the terminal outcome BEFORE the CompleteJob write — if the
    // supervisor dies in between, the reconciler re-reports this stored
    // result rather than losing the measurement. wire_result is the EXACT
    // payload attempted: both CompleteJob and the lost-outcome route
    // replay only identical outcomes, so recovery must resend this object
    // verbatim (frozen file_ops_pending snapshot included), never a
    // reconstruction.
    const j0 = this.journal.read(job.job_id);
    if (j0) this.journal.update(j0, {
      result: { ...result, terminal_status: status, error },
      wire_result: JSON.parse(JSON.stringify(result)) as Record<string, unknown>,
      // The live ledger count, not the frozen wire snapshot: any
      // unresolved admitted effect keeps this journal eligible for
      // resolve-file-op retries after terminal (see journal.unfinished).
      file_ops_pending: pending,
    });

    try {
      await this.client.complete(job.persona_id, job.job_id, this.cfg.runnerID, status, result, error);
      const j = this.journal.read(job.job_id);
      if (j) this.journal.update(j, { status: "reported" });
    } catch (e) {
      // Complete failed — the journal stays 'exited' with the evidence;
      // the reconciler retries rather than re-executing. If the row was
      // swept to 'lost' in the meantime the verdict is immutable: attach
      // the observed outcome via the shared lost-outcome route (identical
      // replay lands; divergent conflicts and is surfaced, not rewritten).
      this.log(`job ${job.job_id} complete: ${e}`);
      try {
        const { job: cur } = await this.client.getJob(job.persona_id, job.job_id);
        if (cur.status === "lost") {
          const j = this.journal.read(job.job_id);
          const r = await this.client.attachLostOutcome(job.persona_id, job.job_id, this.cfg.runnerID, {
            observed_status: status, result: j?.wire_result ?? result, error,
          });
          if (r.status >= 200 && r.status < 300) {
            if (j) this.journal.update(j, { status: "reported", notes: [...j.notes, "outcome attached to lost verdict via shared route"] });
          } else if (j) {
            const why = r.status === 403 ? "not the recorded claimant"
              : r.status === 409 ? "divergent evidence or row not lost"
              : `status=${r.status}`;
            // 403/409 are permanent refusals — the verdict stands and the
            // evidence is durable here; mark reported-with-conflict rather
            // than silently retrying a payload that can never land.
            const permanent = r.status === 403 || r.status === 409;
            this.journal.update(j, {
              ...(permanent ? { status: "reported" as const } : {}),
              notes: [...j.notes, `lost-outcome attach refused (${why}); evidence retained in journal${permanent ? " — permanent refusal, not retried" : ""}`],
            });
          }
        }
      } catch (e2) {
        this.log(`job ${job.job_id} lost-outcome check: ${e2}`);
      }
    }
    await this.recordUsage(job, status, result);
  }

  private async recordUsage(job: JobRow, status: string, result: Record<string, unknown>): Promise<void> {
    const j = this.journal.read(job.job_id);
    const usage = (result.usage ?? {}) as Record<string, unknown>;
    const measured = usage.cpu_ms != null;
    const fact: Record<string, unknown> = {
      fact_id: j?.usage_fact_id ?? `script:${job.job_id}:exec`,
      kind: "script_job",
      phase: "execution",
      funding: { kind: "operator", id: "env" },
      // 'reported' = complete usage report resolved. A script job has
      // genuinely zero model tokens (explicit zero, not a fabricated
      // bill) and carries compute quantities; 'unknown' when wait4
      // rusage was not durably recovered — a later accurate fact can
      // still supersede it.
      status: measured ? "reported" : "unknown",
      input_tokens: 0,
      output_tokens: 0,
      quantities: {
        job_id: job.job_id,
        terminal_status: status,
        ...usage,
      },
    };
    try {
      await this.client.recordUsage(job.persona_id, fact);
      if (j) this.journal.update(j, { usage_status: measured ? "recorded" : "unknown" });
    } catch (e) {
      this.log(`job ${job.job_id} usage: ${e}`);
    }
  }

  /** Wait until the unix socket accepts HTTP, or the child exits. */
  private async waitReady(socketPath: string, spawned: Spawned, timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      try {
        await this.ping(socketPath);
        return;
      } catch {
        if (Date.now() > deadline) throw new Error("workerd did not listen in time");
        // Detect early exit.
        const exited = await Promise.race([spawned.wait.then(() => true), this.sleep(50).then(() => false)]);
        if (exited) throw new Error("workerd exited before listening");
      }
    }
  }

  private ping(socketPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const req = http.request({ socketPath, path: "/ready", method: "GET", timeout: 1000 }, (res) => {
        res.resume();
        resolve();
      });
      req.on("error", reject);
      req.on("timeout", () => { req.destroy(); reject(new Error("timeout")); });
      req.end();
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((r) => setTimeout(r, ms));
  }
}

function spawnedExited(_s: Spawned): boolean {
  return false; // exit status is observed via the wait promise
}
