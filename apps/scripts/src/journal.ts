/**
 * Durable per-job execution journal — the supervisor's recoverable truth.
 * One JSON file per job, written atomically (tmp+rename), surviving the
 * supervisor's death. The journal is what makes "the process exited but
 * CompleteJob never landed" reconcilable: exit evidence and the usage
 * intent persist independently of any single write to the state service.
 *
 * Identity fields (pid, start_ticks, boot_id, unit_name) are recorded at
 * spawn; the reconciler re-verifies them against /proc before touching
 * any process — never a name-wide kill.
 */

import { mkdirSync, readFileSync, renameSync, writeFileSync, readdirSync, existsSync } from "node:fs";
import { join } from "node:path";

export type JournalStatus =
  | "spawned"    // workerd launched, not yet confirmed listening
  | "running"    // request dispatched to the worker
  | "exited"     // process exit observed, evidence captured, not yet reported
  | "reported"   // CompleteJob (or lost-outcome attach) landed
  | "reaped";    // orphan killed by the reconciler

export interface JournalExit {
  code: number | null;
  signal: string | null;
  wall_ms: number;
  cpu_ms: number | null;          // null = not durably recovered
  max_rss_bytes: number | null;   // null = not durably recovered
  rusage_source: "wait4" | "time" | "none"; // how cpu/rss were measured
  killed_by?: "cancel" | "wall_timeout" | "reconcile" | "unknown";
}

export interface JobJournal {
  v: 1;
  job_id: string;
  persona_id: string;
  runner_id: string;
  status: JournalStatus;
  spec: { code_sha256: string };
  // Exact process identity — verified against /proc before any signal.
  pid: number | null;
  start_ticks: number | null;
  boot_id: string | null;
  unit_name: string | null; // systemd scope name, when launched under one
  socket_path: string | null;
  stats_path: string | null;
  cgroup_mode: "systemd" | "prlimit";
  limits: Record<string, number>;
  created_at: string;
  updated_at: string;
  spawned_at: string | null;
  exited_at: string | null;
  exit: JournalExit | null;
  result: Record<string, unknown> | null;
  // The exact `result` object attempted on the wire (CompleteJob result /
  // lost-outcome outcome.result). CompleteJob and AttachLostOutcome both
  // replay ONLY an identical payload — recovery must resend this verbatim,
  // never a reconstruction with fresh file-op counts or added keys.
  wire_result: Record<string, unknown> | null;
  usage_fact_id: string;
  usage_status: "pending" | "recorded" | "unknown";
  file_ops_pending: number | null;
  notes: string[];
}

export class Journal {
  readonly dir: string;
  constructor(dir: string) {
    this.dir = dir;
    mkdirSync(join(dir, "jobs"), { recursive: true });
    mkdirSync(join(dir, "sock"), { recursive: true });
    mkdirSync(join(dir, "tmp"), { recursive: true });
  }

  path(jobID: string): string {
    // Job ids are server-derived and filesystem-safe enough (op:..., j-...)
    // but never trust it: strip anything outside a tight charset.
    const safe = jobID.replace(/[^A-Za-z0-9._:-]/g, "_");
    return join(this.dir, "jobs", `${safe}.json`);
  }

  read(jobID: string): JobJournal | null {
    try {
      return JSON.parse(readFileSync(this.path(jobID), "utf8")) as JobJournal;
    } catch {
      return null;
    }
  }

  write(j: JobJournal): void {
    j.updated_at = new Date().toISOString();
    const p = this.path(j.job_id);
    const tmp = join(this.dir, "tmp", `${j.job_id.replace(/[^A-Za-z0-9._:-]/g, "_")}.${process.pid}.json`);
    writeFileSync(tmp, JSON.stringify(j, null, 2));
    renameSync(tmp, p);
  }

  create(fields: Omit<JobJournal, "v" | "status" | "created_at" | "updated_at" | "spawned_at" | "exited_at" | "exit" | "result" | "wire_result" | "usage_status" | "file_ops_pending" | "notes"> & Partial<JobJournal>): JobJournal {
    const j: JobJournal = {
      v: 1,
      status: "spawned",
      spawned_at: null,
      exited_at: null,
      exit: null,
      result: null,
      wire_result: null,
      usage_status: "pending",
      file_ops_pending: null,
      notes: [],
      ...fields,
      created_at: fields.created_at ?? new Date().toISOString(),
      updated_at: new Date().toISOString(),
    };
    this.write(j);
    return j;
  }

  update(j: JobJournal, patch: Partial<JobJournal>): JobJournal {
    Object.assign(j, patch);
    this.write(j);
    return j;
  }

  /** Journals still carrying recoverable execution work. */
  unfinished(): JobJournal[] {
    const out: JobJournal[] = [];
    const jobsDir = join(this.dir, "jobs");
    if (!existsSync(jobsDir)) return out;
    for (const name of readdirSync(jobsDir)) {
      if (!name.endsWith(".json")) continue;
      try {
        const j = JSON.parse(readFileSync(join(jobsDir, name), "utf8")) as JobJournal;
        if (j.status !== "reported" || j.usage_status !== "recorded") out.push(j);
      } catch { /* corrupt entry: skip, never guess */ }
    }
    return out.sort((a, b) => a.created_at.localeCompare(b.created_at));
  }

  socketPath(jobID: string): string {
    const safe = jobID.replace(/[^A-Za-z0-9._:-]/g, "_");
    return join(this.dir, "sock", `${safe}.sock`);
  }

  statsPath(jobID: string): string {
    const safe = jobID.replace(/[^A-Za-z0-9._:-]/g, "_");
    return join(this.dir, "tmp", `${safe}.rusage`);
  }
}
