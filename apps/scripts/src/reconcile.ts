/**
 * Reconciliation: the journal + the shared attention route are the
 * recoverable truth, bridging "the supervisor died" and "the state
 * service knows the truth". Two pass kinds share the machinery:
 *
 *  - run() — STARTUP pass. The previous supervisor is dead by
 *    construction, so a journaled spawned/running process is restart
 *    evidence: verify its recorded identity against /proc and reap it
 *    (never a name-wide kill). No re-execution: an indeterminate
 *    backend state is not permission to rerun.
 *  - runRecovery(active) — PERIODIC pass while this supervisor lives.
 *    Job ids in `active` belong to this process's own executions and
 *    are never touched. A spawned/running journal outside `active` is
 *    indeterminate, NOT orphan evidence — a periodic pass must not
 *    kill what it cannot prove is orphaned, so such rows are left to
 *    the claim-expiry sweep and the lost-outcome attach.
 *
 * Both passes then deliver what is durably owed, each obligation
 * independently retryable until resolved or permanently refused:
 *
 *  - result delivery: 'exited' journals re-report the stored outcome —
 *    the EXACT first-attempt wire payload (journal.wire_result), since
 *    CompleteJob and AttachLostOutcome only replay identical outcomes.
 *  - lost-outcome attachment: a swept 'lost' job keeps its immutable
 *    verdict; the observed outcome attaches under result.observed_
 *    outcome. 2xx = attached; 403/409 = permanent refusal — surfaced
 *    in notes and marked reported-with-conflict rather than retried
 *    forever; anything else stays eligible.
 *  - usage facts: resent while usage_status != 'recorded'; 'unknown'
 *    is honest when no wait4 evidence survived.
 *  - pending file effects: the ledger's keyed resend decides what
 *    landed; it may settle ops even after the job is terminal.
 *  - attention rows without a journal: 'lost' rows get a durable
 *    journal created BEFORE any attempt (so the outcome, usage intent
 *    and conflict evidence survive another crash); running/
 *    cancel_requested rows are DEFERRED — a missing journal is not
 *    proof the process stopped, so nothing is terminalized on absent
 *    evidence; the lease-expiry sweep turns them 'lost' and the
 *    honest no-evidence outcome attaches then.
 */

import { StateClient, type JobRow } from "./api.ts";
import { Journal, type JobJournal } from "./journal.ts";
import { verifyIdentity, killVerified, findDescendant, startTicks } from "./proc.ts";

export interface ReconcileConfig {
  client: StateClient;
  journal: Journal;
  runnerID: string;
  log?: (line: string) => void;
  attentionPage?: number;    // rows per attention page (default 64)
  attentionMaxPages?: number; // pages per pass (default 8); the cursor
                              // persists across passes so backlogs
                              // deeper than one pass still progress.
}

export interface PassStats {
  reaped: number;
  reported: number;
  failed: number;
  attention: { seen: number; resolved: number; pending: number; failed: number };
}

type AttachOutcome = "attached" | "refused_permanent" | "route_missing" | "transient";
type RowOutcome = "resolved" | "pending";

export class Reconciler {
  private log: (line: string) => void;
  private attAfterPersona = "";
  private attAfterJob = "";

  readonly cfg: ReconcileConfig;
  constructor(cfg: ReconcileConfig) {
    this.cfg = cfg;
    this.log = cfg.log ?? ((l) => console.log(`[reconcile] ${l}`));
  }

  /** Startup pass: orphan reaping IS valid restart evidence here. */
  async run(): Promise<PassStats> {
    return this.pass(new Set(), true);
  }

  /** Periodic pass: never reaps; `active` ids are this supervisor's own
   *  live executions and are skipped entirely. */
  async runRecovery(active: ReadonlySet<string>): Promise<PassStats> {
    return this.pass(active, false);
  }

  private async pass(active: ReadonlySet<string>, reapOrphans: boolean): Promise<PassStats> {
    let reaped = 0, reported = 0, failed = 0;
    for (const j of this.cfg.journal.unfinished()) {
      if (active.has(j.job_id)) continue;
      try {
        if (!reapOrphans && (j.status === "spawned" || j.status === "running")) {
          // Live-process evidence without an active owner mid-run is
          // indeterminate — never signal it; the lease expiry sweeps the
          // claim to 'lost' and the outcome attaches then.
          continue;
        }
        await this.reconcileOne(j);
        if (j.status === "reaped") reaped++;
        if (j.status === "reported") reported++;
      } catch (e) {
        failed++;
        this.log(`job ${j.job_id}: ${e}`);
      }
    }
    const attention = await this.attentionPass(active, reapOrphans);
    return { reaped, reported, failed, attention };
  }

  /**
   * Walk the shared /internal/core/jobs/attention route continuing from
   * the persisted (persona,job) cursor — a backlog deeper than one
   * bounded pass keeps progressing across passes instead of restarting
   * at the same prefix. A row that cannot resolve counts 'pending' (or
   * 'failed' on a thrown error) but never blocks the cursor, so an
   * unresolvable prefix cannot starve later work.
   */
  private async attentionPass(active: ReadonlySet<string>, reapOrphans: boolean): Promise<PassStats["attention"]> {
    const PAGE = this.cfg.attentionPage ?? 64;
    const MAX_PAGES = this.cfg.attentionMaxPages ?? 8;
    const out = { seen: 0, resolved: 0, pending: 0, failed: 0 };
    for (let p = 0; p < MAX_PAGES; p++) {
      let res;
      try {
        res = await this.cfg.client.attentionJobs(this.cfg.runnerID, ["script"], PAGE, this.attAfterPersona, this.attAfterJob);
      } catch (e) {
        this.log(`attention: ${e}`);
        break;
      }
      for (const row of res.jobs) {
        out.seen++;
        if (active.has(row.job_id)) {
          out.pending++; // our own live job — the runner settles it
          continue;
        }
        try {
          const r = await this.reconcileAttention(row, reapOrphans);
          if (r === "resolved") out.resolved++; else out.pending++;
        } catch (e) {
          out.failed++;
          this.log(`attention job ${row.job_id}: ${e}`);
        }
      }
      if (!res.next) {
        // Short/empty page: the whole unresolved set was walked — wrap
        // so the next pass starts from the beginning again.
        this.attAfterPersona = "";
        this.attAfterJob = "";
        break;
      }
      this.attAfterPersona = res.next.after_persona;
      this.attAfterJob = res.next.after_job;
    }
    if (out.seen > 0 || out.failed > 0) {
      this.log(`attention: seen=${out.seen} resolved=${out.resolved} pending=${out.pending} failed=${out.failed}`);
    }
    return out;
  }

  /**
   * One unresolved backend row. Returns 'resolved' only when nothing
   * durable remains owed — a caught inner failure leaves the row
   * 'pending' and eligible for the next pass.
   */
  private async reconcileAttention(row: JobRow, reapOrphans: boolean): Promise<RowOutcome> {
    const j = this.cfg.journal.read(row.job_id);
    if (j) {
      if (!reapOrphans && (j.status === "spawned" || j.status === "running")) {
        return "pending"; // indeterminate mid-run: hands off
      }
      await this.reconcileOne(j);
      // A row sitting 'lost' still owes the observed outcome until the
      // attach lands (or is permanently refused — recorded in notes).
      // reconcileOne already attempts this for 'exited'/'reaped'
      // journals; the check here also covers journals that reached
      // 'reported' while the attach was still owed.
      const lostAttachOwed = row.status === "lost" && j.result != null;
      if (lostAttachOwed && !this.attachResolved(j)) {
        const r = await this.attachLost(j, String(j.result!.terminal_status ?? "failed"), this.payloadFor(j));
        if ((r === "attached" || r === "refused_permanent") && (j.status === "exited" || j.status === "reaped")) {
          this.cfg.journal.update(j, { status: "reported" });
        }
      }
      const settled = this.journalSettled(j) && (!lostAttachOwed || this.attachResolved(j));
      return settled ? "resolved" : "pending";
    }

    // No local journal: settle the API-ledger file ops for this claim —
    // keyed resends decide what landed; they are never re-executed.
    const pending = await this.resolvePendingOps(row.persona_id, row.job_id);

    if (row.status === "lost") {
      // The sweep already gave the honest indeterminate verdict. Create
      // the durable journal BEFORE attempting anything, so the exact
      // outcome payload, usage intent and conflict evidence survive a
      // crash between attach and usage.
      const wireResult: Record<string, unknown> = {
        reason: "runner_restart_no_journal",
        detail: "claim unresolved but this runner has no execution journal — outcome indeterminate",
        file_ops_pending: pending,
      };
      const j2 = this.cfg.journal.create({
        job_id: row.job_id,
        persona_id: row.persona_id,
        runner_id: this.cfg.runnerID,
        pid: null,
        start_ticks: null,
        boot_id: null,
        unit_name: null,
        socket_path: null,
        stats_path: null,
        cgroup_mode: "prlimit",
        limits: {},
        spec: { code_sha256: "0".repeat(64) },
        usage_fact_id: `script:${row.job_id}:exec`,
      });
      this.cfg.journal.update(j2, {
        status: "exited",
        result: { ...wireResult, terminal_status: "lost", error: "no execution evidence after restart" },
        wire_result: wireResult,
        notes: ["no local journal — durable record created for lost-claim evidence attach"],
      });
      await this.reportAndUsage(j2, "lost", wireResult);
      return this.journalSettled(j2) ? "resolved" : "pending";
    }

    // running / cancel_requested (or anything else unresolved): a
    // missing journal is NOT proof the process stopped. Terminalizing
    // here could declare a live execution cancelled/failed on absent
    // evidence. Leave the claim to the lease-expiry sweep; once 'lost'
    // the no-evidence outcome attaches above. Unsupported destruction
    // case: if the workDir was wiped while a process survived, the
    // orphan cannot be identified — the claim still resolves to 'lost'
    // honestly, and the process is bounded by its own rlimits/scope.
    this.log(`attention job ${row.job_id}: status=${row.status} with no journal — indeterminate, left for lease-expiry sweep (not terminalized)`);
    return "pending";
  }

  /** Nothing durable left to deliver for this journal — reported and a
   *  usage fact recorded (a recorded 'unknown' IS delivered; 'pending'
   *  means the send never landed). */
  private journalSettled(j: JobJournal): boolean {
    const cur = this.cfg.journal.read(j.job_id) ?? j;
    return cur.status === "reported" && cur.usage_status !== "pending";
  }

  /** Whether the lost-outcome attach debt is durably discharged —
   *  either attached or permanently refused (the refusal note is the
   *  visible record; transient and route-missing stay retryable). */
  private attachResolved(j: JobJournal): boolean {
    return j.notes.some((n) => n.includes("outcome attached to lost") || n.includes("attach refused"));
  }

  private async reconcileOne(j: JobJournal): Promise<void> {
    // 0. Pending file operations resolve FIRST, whatever the job's state.
    //    An admitted op's upstream effect may have landed while the
    //    supervisor was dead; only the keyed resend answers "did it
    //    commit" truthfully — a dead connection is not proof it did not.
    //    The resolve route does not require a live claim, so ops settle
    //    even after the job is terminal/lost. Never re-executed, never
    //    replayed as a fresh operation.
    const pending = await this.resolvePendingOps(j.persona_id, j.job_id, j);

    // 1. A live orphan process: kill by verified identity.
    if ((j.status === "spawned" || j.status === "running") && j.pid != null) {
      const identity = { pid: j.pid, start_ticks: j.start_ticks, boot_id: j.boot_id };
      if (verifyIdentity(identity)) {
        this.log(`job ${j.job_id}: reaping orphan pid=${j.pid}`);
        // The journal pid may be the launcher (runlimited) if the
        // supervisor died before the workerd-pid update — kill the
        // workerd descendant first so nothing survives the wrapper.
        const workerPid = findDescendant(j.pid, "workerd");
        if (workerPid != null) {
          killVerified({ pid: workerPid, start_ticks: startTicks(workerPid), boot_id: j.boot_id }, "SIGKILL");
        }
        killVerified(identity, "SIGKILL");
        this.cfg.journal.update(j, { status: "reaped" });
        j.status = "reaped";
      } else {
        // Identity didn't verify — either the process exited on its own
        // or the pid was recycled. Record the doubt, do not touch it.
        this.cfg.journal.update(j, {
          status: "reaped",
          notes: [...j.notes, "identity not verified at reconcile; left untouched"],
        });
        j.status = "reaped";
      }
      // Fetch the row: if the claim already swept to 'lost', the verdict
      // stands; we still record usage evidence below.
      await this.reportAndUsage(j, "failed", {
        reason: "supervisor_restarted",
        detail: "orphan reaped on supervisor start; backend outcome indeterminate",
        file_ops_pending: pending,
      });
      return;
    }

    // 2. Exited but never reported: re-report the stored outcome — the
    //    exact first-attempt payload, not a reconstruction.
    if (j.status === "exited" && j.result) {
      const status = String(j.result.terminal_status ?? "failed");
      await this.reportAndUsage(j, status, this.payloadFor(j));
      return;
    }

    // 3. Only usage pending.
    if (j.usage_status !== "recorded") {
      await this.sendUsage(j);
    }
  }

  /**
   * The exact payload this job must (re)send: journal.wire_result when
   * present (frozen at first attempt); otherwise the journal's stored
   * result minus its local bookkeeping keys — the deterministic
   * reconstruction that matches what finish() first attempted. Fresh
   * file-op counts are NEVER injected: the producer replays only
   * identical outcomes, and the live ledger (queried every pass) is
   * where post-attempt settlement remains visible.
   */
  private payloadFor(j: JobJournal): Record<string, unknown> {
    if (j.wire_result) return j.wire_result;
    const { terminal_status: _ts, error: _e, ...rest } = j.result ?? {};
    return rest;
  }

  /**
   * Settle the job's admitted/unknown file ops through the keyed resend
   * route — the ledger row + filesvc receipt decide what happened, never
   * an assumption. Returns the post-resolve pending count (null when the
   * ledger itself was unreachable — reported as unknown, not zero).
   */
  private async resolvePendingOps(personaID: string, jobID: string, j?: JobJournal): Promise<number | null> {
    try {
      const { ops } = await this.cfg.client.listFileOps(personaID, jobID, true);
      for (const op of ops) {
        try {
          await this.cfg.client.resolveFileOp(personaID, jobID, op.op_id, this.cfg.runnerID);
        } catch (e) {
          this.log(`job ${jobID}: resolve op ${op.op_id}: ${e}`);
        }
      }
      // Re-read the truth — a resolve call can itself lose its upstream
      // response; the count is whatever the ledger actually says.
      const after = await this.cfg.client.listFileOps(personaID, jobID, true);
      if (j) this.cfg.journal.update(j, { file_ops_pending: after.pending });
      return after.pending;
    } catch (e) {
      this.log(`job ${jobID}: file ops listing: ${e}`);
      return null;
    }
  }

  private async reportAndUsage(j: JobJournal, status: string, result: Record<string, unknown>): Promise<void> {
    // Check the row first — a 'lost' verdict is preserved, never
    // rewritten to the runner's observed outcome.
    let row: { status: string } | null = null;
    try {
      const { job } = await this.cfg.client.getJob(j.persona_id, j.job_id);
      row = job;
    } catch (e) {
      this.log(`job ${j.job_id}: getJob ${e}`);
    }

    // First attempt for this journal? Freeze the exact payload before
    // it goes on the wire so every later retry is byte-identical.
    if (!j.wire_result) {
      this.cfg.journal.update(j, {
        result: { ...result, terminal_status: status },
        wire_result: JSON.parse(JSON.stringify(result)) as Record<string, unknown>,
      });
    }
    const payload = this.payloadFor(j);

    if (row && (row.status === "queued" || row.status === "running" || row.status === "cancel_requested")) {
      // Still ours or claim still open — attempt the terminal report.
      try {
        await this.cfg.client.complete(
          j.persona_id, j.job_id, this.cfg.runnerID, status === "cancelled" ? "cancelled" : status,
          payload, String(j.result?.error ?? ""),
        );
        if (j.status === "exited" || j.status === "reaped") {
          this.cfg.journal.update(j, { status: "reported" });
        }
      } catch (e) {
        this.log(`job ${j.job_id}: complete ${e} (kept for retry)`);
      }
    } else if (row === null) {
      // The row could not be read — the outcome was NOT delivered.
      // Leaving the journal unfinished keeps it eligible for the next
      // pass; marking it reported here would drop the only evidence.
      this.log(`job ${j.job_id}: row unreadable — outcome still owed, kept for retry`);
    } else {
      // Terminal already (done/failed/cancelled/lost): do not rewrite.
      if (row.status === "lost") {
        // 'lost' is an immutable verdict — CompleteJob conflicts on it.
        // Attach the observed outcome through the shared lost-outcome
        // route; transient refusals keep the journal eligible, permanent
        // ones are surfaced and stop the retry.
        const r = await this.attachLost(j, status, payload);
        if (r === "attached" || r === "refused_permanent") {
          if (j.status === "exited" || j.status === "reaped") {
            this.cfg.journal.update(j, { status: "reported" });
          }
        }
      } else if (j.status === "exited" || j.status === "reaped") {
        this.cfg.journal.update(j, { status: "reported", notes: [...j.notes, `row already ${row.status} at reconcile`] });
      }
    }
    await this.sendUsage(j);
  }

  /**
   * Attach the observed outcome to a swept-'lost' job through the shared
   * seam. Returns the classification so callers can decide whether the
   * delivery obligation is discharged (attached / permanently refused —
   * surfaced) or still pending (route missing / transient).
   */
  private async attachLost(j: JobJournal, status: string, result: Record<string, unknown>): Promise<AttachOutcome> {
    try {
      const res = await this.cfg.client.attachLostOutcome(j.persona_id, j.job_id, this.cfg.runnerID, {
        observed_status: status,
        result,
        error: String(j.result?.error ?? result.error ?? ""),
      });
      return this.attachStatus(j, res.status);
    } catch (e) {
      this.log(`job ${j.job_id}: lost-outcome attach ${e}`);
      return "transient";
    }
  }

  /**
   * Classify a lost-outcome attach response. 2xx = attached/replayed;
   * 403 = we are not the recorded claimant (permanent — runner identity
   * changed); 409 = not-lost or divergent evidence (permanent refusal —
   * the verdict stands, we do not retry a conflicting attach); 404/405/
   * 501 = route not deployed; anything else = transient, kept for retry.
   * Permanent outcomes are written into the journal notes so the
   * conflict is visible, not silently rewritten forever.
   */
  private attachStatus(j: JobJournal, status: number): AttachOutcome {
    const note = (s: string) => {
      this.cfg.journal.update(j, { notes: [...j.notes, s] });
    };
    if (status >= 200 && status < 300) {
      note("observed outcome attached to lost job");
      return "attached";
    }
    if (status === 403) {
      this.log(`job ${j.job_id}: lost-outcome attach 403 — not the recorded claimant`);
      note("lost-outcome attach refused: not the recorded claimant (runner identity changed before this restart?) — permanent, evidence retained");
      return "refused_permanent";
    }
    if (status === 409) {
      this.log(`job ${j.job_id}: lost-outcome attach 409 — conflicting evidence or not-lost`);
      note("lost-outcome attach refused: divergent evidence or row not lost — verdict untouched, kept as evidence (permanent)");
      return "refused_permanent";
    }
    if (status === 404 || status === 405 || status === 501) {
      this.log(`job ${j.job_id}: lost-outcome route not wired — evidence kept in journal + usage facts`);
      note("lost verdict preserved; outcome attach needs shared AttachLostOutcome route (see api.ts contract)");
      return "route_missing";
    }
    this.log(`job ${j.job_id}: lost-outcome attach refused ${status} — kept for retry`);
    note(`lost-outcome attach status=${status}; evidence retained for retry`);
    return "transient";
  }

  private async sendUsage(j: JobJournal): Promise<void> {
    const usage = (j.result?.usage ?? j.wire_result?.usage ?? {}) as Record<string, unknown>;
    const measured = usage.cpu_ms != null;
    try {
      await this.cfg.client.recordUsage(j.persona_id, {
        fact_id: j.usage_fact_id,
        kind: "script_job",
        phase: "execution",
        funding: { kind: "operator", id: "env" },
        status: measured ? "reported" : "unknown",
        input_tokens: 0,
        output_tokens: 0,
        quantities: { job_id: j.job_id, terminal_status: j.result?.terminal_status ?? "unknown", ...usage },
      });
      this.cfg.journal.update(j, { usage_status: measured ? "recorded" : "unknown" });
    } catch (e) {
      this.log(`job ${j.job_id}: usage ${e}`);
    }
  }
}
