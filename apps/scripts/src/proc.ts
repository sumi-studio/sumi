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

/** Walk a process tree depth-first for a pid whose comm matches. */
export function findDescendant(rootPid: number, comm: string, maxDepth = 4): number | null {
  let frontier = [rootPid];
  for (let d = 0; d < maxDepth; d++) {
    const next: number[] = [];
    for (const p of frontier) {
      for (const c of childrenOf(p)) {
        if (commOf(c) === comm) return c;
        next.push(c);
      }
    }
    frontier = next;
  }
  return null;
}

/** True when process group `pgid` has a live member whose comm matches
 *  — proof the group still holds a spawn's payload. This is the only
 *  safe check before killpg on a stale pgid: a group id recycled after
 *  the whole original group died would not contain our workerd. */
export function groupHasComm(pgid: number, comm: string): boolean {
  try {
    for (const name of readdirSync("/proc")) {
      if (!/^\d+$/.test(name)) continue;
      try {
        const stat = readFileSync(`/proc/${name}/stat`, "utf8");
        const after = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
        // fields after comm: state(3) ppid(4) pgrp(5) — index 2 = pgrp.
        const state = after[0];
        if (state === "Z" || state === "X") continue;
        if (Number(after[2]) !== pgid) continue;
        if (commOf(Number(name)) === comm) return true;
      } catch { /* went away mid-scan */ }
    }
  } catch { /* /proc unreadable */ }
  return false;
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
