/**
 * Process identity: exact-PID operations only, verified against boot id
 * and /proc start ticks — never a command-name-wide match. A PID recycled
 * to a different process fails the identity check and is left alone.
 */

import { readFileSync, readdirSync } from "node:fs";
import { kill } from "node:process";

export function bootID(): string | null {
  try {
    return readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
  } catch {
    return null;
  }
}

/** Field 22 of /proc/<pid>/stat: start time in clock ticks since boot. */
export function startTicks(pid: number): number | null {
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
    // comm may contain spaces/parens — field 22 follows the last ')'.
    const after = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
    // fields: state(3) ppid(4) pgrp(5) session(6) tty_nr(7) tpgid(8) flags(9)
    // minflt(10) cminflt(11) majflt(12) cmajflt(13) utime(14) stime(15)
    // cutime(16) cstime(17) priority(18) nice(19) num_threads(20)
    // itrealvalue(21) starttime(22)
    const ticks = Number(after[19]);
    return Number.isFinite(ticks) ? ticks : null;
  } catch {
    return null;
  }
}

/** The process's argv as one string (NUL-separated on disk). Empty for
 *  kernel threads/zombies and when unreadable — never throws. */
export function cmdlineOf(pid: number): string {
  try {
    return readFileSync(`/proc/${pid}/cmdline`, "utf8").replace(/\0/g, " ");
  } catch {
    return "";
  }
}

/** argv[0] — the verbatim exec path. Survives a process renaming its
 *  comm via prctl; empty for kernel threads/zombies/unreadable. */
export function argv0Of(pid: number): string {
  try {
    return readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0")[0] ?? "";
  } catch {
    return "";
  }
}

/** The comm a process gets when exec'd from `exePath`: the basename
 *  truncated to TASK_COMM_LEN-1 (15 chars). A versioned binary path
 *  like `workerd-2026-08-04` yields comm `workerd-2026-08` — callers
 *  matching process names must derive it, never assume a fixed name. */
export function commForExe(exePath: string): string {
  const base = exePath.split("/").pop() ?? exePath;
  return base.slice(0, 15);
}

/** True when `pid` is the image exec'd from `exePath`: comm is the
 *  basename truncated to TASK_COMM_LEN-1, and argv[0] is the verbatim
 *  exec path (which also survives a self-renamed comm). Matching only
 *  a bare basename like "workerd" fails for every versioned path the
 *  config legitimately supplies (workerd-2026-08-04 → comm
 *  workerd-2026-08) — identity must be derived, not assumed. */
export function isExeImage(pid: number, exePath: string): boolean {
  if (commOf(pid) === commForExe(exePath)) return true;
  return argv0Of(pid) === exePath;
}

export function commOf(pid: number): string | null {
  try {
    return readFileSync(`/proc/${pid}/comm`, "utf8").trim();
  } catch {
    return null;
  }
}

export function alive(pid: number): boolean {
  try {
    kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Child pids of a process (Linux /proc children file). */
export function childrenOf(pid: number): number[] {
  try {
    const tasks = readdirSync(`/proc/${pid}/task`);
    const out: number[] = [];
    for (const t of tasks) {
      try {
        const c = readFileSync(`/proc/${pid}/task/${t}/children`, "utf8").trim();
        for (const p of c.split(/\s+/)) {
          if (p) out.push(Number(p));
        }
      } catch { /* thread went away */ }
    }
    return out;
  } catch {
    return [];
  }
}

/** Walk a process tree depth-first for the first pid matching `pred`. */
export function findDescendantWhere(rootPid: number, pred: (pid: number) => boolean, maxDepth = 4): number | null {
  let frontier = [rootPid];
  for (let d = 0; d < maxDepth; d++) {
    const next: number[] = [];
    for (const p of frontier) {
      for (const c of childrenOf(p)) {
        if (pred(c)) return c;
        next.push(c);
      }
    }
    frontier = next;
  }
  return null;
}

/** Walk a process tree depth-first for a pid whose comm matches. */
export function findDescendant(rootPid: number, comm: string, maxDepth = 4): number | null {
  return findDescendantWhere(rootPid, (c) => commOf(c) === comm, maxDepth);
}

export interface GroupMember {
  pid: number;
  comm: string | null;
  startTicks: number | null;
}

/** Live members of process group `pgid` (zombies excluded — they hold
 *  no signalable payload). Membership alone does NOT prove the group is
 *  ours: a comm match is just a name, and a journal survives arbitrary
 *  downtime across pid/pgid recycling. Callers must establish ownership
 *  separately (leader identity + fork-order ticks + boot id). */
export function groupMembers(pgid: number): GroupMember[] {
  const out: GroupMember[] = [];
  try {
    for (const name of readdirSync("/proc")) {
      if (!/^\d+$/.test(name)) continue;
      const pid = Number(name);
      try {
        const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
        const after = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        // fields after comm: state(3) ppid(4) pgrp(5) — index 2 = pgrp.
        const state = after[0];
        if (state === "Z" || state === "X") continue;
        if (Number(after[2]) !== pgid) continue;
        out.push({ pid, comm: commOf(pid), startTicks: Number(after[19]) });
      } catch { /* went away mid-scan */ }
    }
  } catch { /* /proc unreadable */ }
  return out;
}

export interface ProcessIdentity {
  pid: number;
  start_ticks: number | null;
  boot_id: string | null;
}

/** Verify a live process is exactly the recorded one. */
export function verifyIdentity(id: ProcessIdentity): boolean {
  if (!alive(id.pid)) return false;
  if (id.boot_id && bootID() !== id.boot_id) return false;
  if (id.start_ticks != null && startTicks(id.pid) !== id.start_ticks) return false;
  return true;
}

/** SIGKILL exactly the recorded process, after identity verification. */
export function killVerified(id: ProcessIdentity, signal: NodeJS.Signals = "SIGKILL"): boolean {
  if (!verifyIdentity(id)) return false;
  try {
    kill(id.pid, signal);
    return true;
  } catch {
    return false;
  }
}
