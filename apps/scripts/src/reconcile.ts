/**
 * Startup reconciliation. The journal survives the supervisor; this pass
 * closes the gap between "the supervisor died" and "the state service
 * knows the truth":
 *
 *  - Journal says a process was spawned/running: verify the recorded
 *    identity against /proc. If it is still alive it is an orphan —
 *    kill it by verified identity (never by name), mark 'reaped', and
 *    let the job's expired claim sweep to 'lost' with the observed
 *    outcome recorded in the journal + usage evidence. We do NOT
 *    re-execute: an indeterminate backend state is not permission to
 *    rerun.
 *  - Journal says 'exited' but not 'reported': the CompleteJob write
 *    never landed. Re-report the stored outcome (idempotent resend —
 *    identical terminal reports replay, divergent ones conflict and are
 *    surfaced, not retried).
 *  - usage_status != 'recorded': resend the usage fact. An earlier
 *    'unknown' fact is superseded by 'reported' when measured evidence
 *    is recovered — the ledger handles that; divergent facts conflict
 *    and stay divergent.
 *  - 'lost' jobs this runner once owned: if the shared AttachLostOutcome
 *    route is wired by root, the observed outcome attaches; until then
 *    the evidence stays durable in the journal + usage facts and the
 *    job's verdict stays 'lost' (never rewritten to success).
 */

import { StateClient } from "./api.ts";
import { Journal, type JobJournal } from "./journal.ts";
import { verifyIdentity, killVerified, findDescendant, startTicks } from "./proc.ts";

export interface ReconcileConfig {
  client: StateClient;
  journal: Journal;
  runnerID: string;
  log?: (line: string) => void;
}

export class Reconciler {
  private log: (line: string) => void;

  readonly cfg: ReconcileConfig;
  constructor(cfg: ReconcileConfig) {
    this.cfg = cfg;
    this.log = cfg.log ?? ((l) => console.log(`[reconcile] ${l}`));
  }

  async run(): Promise<{ reaped: number; reported: number; failed: number }> {
    let reaped = 0, reported = 0, failed = 0;
    for (const j of this.cfg.journal.unfinished()) {
      try {
        await this.reconcileOne(j);
        if (j.status === "reaped") reaped++;
        if (j.status === "reported") reported++;
      } catch (e) {
        failed++;
        this.log(`job ${j.job_id}: ${e}`);
      }
    }
    return { reaped, reported, failed };
  }

  private async reconcileOne(j: JobJournal): Promise<void> {
    // 0. Pending file operations resolve FIRST, whatever the job's state.
    //    An admitted op's upstream effect may have landed while the
    //    supervisor was dead; only the keyed resend answers "did it
    //    commit" truthfully — a dead connection is not proof it did not.
    //    The resolve route does not require a live claim, so ops settle
    //    even after the job is terminal/lost. Never re-executed, never
    //    replayed as a fresh operation.
    const pending = await this.resolvePendingOps(j);

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

    // 2. Exited but never reported: re-report the stored outcome.
    if (j.status === "exited" && j.result) {
      const status = String(j.result.terminal_status ?? "failed");
      await this.reportAndUsage(j, status, { ...j.result, file_ops_pending: pending });
      return;
    }

    // 3. Only usage pending.
    if (j.usage_status !== "recorded") {
      await this.sendUsage(j);
    }
  }

  /**
   * Settle the job's admitted/unknown file ops through the keyed resend
   * route — the ledger row + filesvc receipt decide what happened, never
   * an assumption. Returns the post-resolve pending count (null when the
   * ledger itself was unreachable — reported as unknown, not zero).
   */
  private async resolvePendingOps(j: JobJournal): Promise<number | null> {
    try {
      const { ops } = await this.cfg.client.listFileOps(j.persona_id, j.job_id, true);
      for (const op of ops) {
        try {
          await this.cfg.client.resolveFileOp(j.persona_id, j.job_id, op.op_id, this.cfg.runnerID);
        } catch (e) {
          this.log(`job ${j.job_id}: resolve op ${op.op_id}: ${e}`);
        }
      }
      // Re-read the truth — a resolve call can itself lose its upstream
      // response; the count is whatever the ledger actually says.
      const after = await this.cfg.client.listFileOps(j.persona_id, j.job_id, true);
      this.cfg.journal.update(j, { file_ops_pending: after.pending });
      return after.pending;
    } catch (e) {
      this.log(`job ${j.job_id}: file ops listing: ${e}`);
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

    if (row && (row.status === "queued" || row.status === "running" || row.status === "cancel_requested")) {
      // Still ours or claim still open — attempt the terminal report.
      try {
        await this.cfg.client.complete(
          j.persona_id, j.job_id, this.cfg.runnerID, status === "cancelled" ? "cancelled" : status,
          { ...result, terminal_status: status }, String(result.error ?? ""),
        );
        if (j.status === "exited") this.cfg.journal.update(j, { status: "reported" });
      } catch (e) {
        this.log(`job ${j.job_id}: complete ${e} (kept for retry)`);
      }
    } else {
      // Terminal already (done/failed/cancelled/lost): do not rewrite.
      if (row?.status === "lost") {
        // 'lost' is an immutable verdict — CompleteJob conflicts on it.
        // Attach the observed outcome through the shared lost-outcome
        // route when wired; until then the evidence stays durable in
        // this journal + the usage fact below (which does attach today —
        // RecordUsage is persona-scoped, not claim-fenced).
        await this.attachLost(j, status, result);
      }
      if (j.status === "exited") {
        this.cfg.journal.update(j, { status: "reported", notes: [...j.notes, `row already ${row?.status ?? "unreadable"} at reconcile`] });
      }
    }
    await this.sendUsage(j);
  }

  /**
   * Attach the observed outcome to a swept-'lost' job through the shared
   * seam. The route is owned by the shared job implementation; until it
   * exists the evidence stays durable here — never a verdict rewrite,
   * never a silent drop.
   */
  private async attachLost(j: JobJournal, status: string, result: Record<string, unknown>): Promise<void> {
    try {
      const res = await this.cfg.client.attachLostOutcome(j.persona_id, j.job_id, this.cfg.runnerID, {
        observed_status: status,
        result,
        error: String(result.error ?? ""),
      });
      if (res.status === 404 || res.status === 501 || res.status === 405) {
        this.log(`job ${j.job_id}: lost-outcome route not wired — evidence kept in journal + usage facts`);
        this.cfg.journal.update(j, {
          notes: [...j.notes, "lost verdict preserved; outcome attach needs shared AttachLostOutcome route (see api.ts contract)"],
        });
      } else if (res.status >= 200 && res.status < 300) {
        this.cfg.journal.update(j, { notes: [...j.notes, "observed outcome attached to lost job"] });
      } else {
        this.log(`job ${j.job_id}: lost-outcome attach refused ${res.status} — kept for retry`);
      }
    } catch (e) {
      this.log(`job ${j.job_id}: lost-outcome attach ${e}`);
    }
  }

  private async sendUsage(j: JobJournal): Promise<void> {
    const usage = (j.result?.usage ?? {}) as Record<string, unknown>;
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
