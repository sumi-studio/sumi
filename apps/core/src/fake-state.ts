import { jsonEqual } from "./json.ts";
import { FencedError, type StateClient, StateError } from "./state-client.ts";
import type {
  Approval,
  ApprovalDecision,
  CommitRequest,
  Event,
  Input,
  Job,
  JobTerminalReport,
  Json,
  LoadResult,
  ModelBinding,
  Operation,
  OutboxEntry,
  PersonaState,
  PlanCall,
  RecoverResult,
  Schedule,
  Turn,
  TurnPlan,
  WriterLease,
} from "./types.ts";

const MISS_POLICIES = new Set([
  "fire_late",
  "coalesce",
  "expire",
  "report_missed",
]);

const JOB_TERMINAL = new Set(["done", "failed", "cancelled", "lost"]);

/** Mirrors Go validateJobRequest for kind 'subprocess'. */
function validateSubprocessRequest(request: Record<string, unknown>) {
  const cmd = request.command;
  if (
    !Array.isArray(cmd) ||
    cmd.length === 0 ||
    cmd.some((a) => typeof a !== "string" || a === "")
  ) {
    throw new StateError(
      400,
      "subprocess job requires a non-empty command array of strings",
    );
  }
  if (request.cwd !== undefined && typeof request.cwd !== "string") {
    throw new StateError(400, "subprocess cwd must be a string");
  }
  const t = request.timeout_ms;
  if (
    t !== undefined &&
    (typeof t !== "number" || !Number.isInteger(t) || t <= 0 || t > 3_600_000)
  ) {
    throw new StateError(
      400,
      "subprocess timeout_ms must be an integer in (0, 3600000]",
    );
  }
  if (request.env !== undefined) {
    if (typeof request.env !== "object" || request.env === null) {
      throw new StateError(400, "subprocess env must be an object of strings");
    }
    for (const [k, v] of Object.entries(request.env)) {
      if (typeof v !== "string") {
        throw new StateError(400, `subprocess env[${k}] must be a string`);
      }
    }
  }
}

/** True when any string in a JSON-shaped value contains NUL. */
function hasNul(v: unknown): boolean {
  if (typeof v === "string") return v.includes("\u0000");
  if (Array.isArray(v)) return v.some(hasNul);
  if (v !== null && typeof v === "object") {
    return Object.values(v).some(hasNul);
  }
  return false;
}

/** Matches Go retryBackoff: 200ms doubling per attempt, capped at 30s. */
function retryBackoffMs(attempt: number): number {
  const shift = Math.min(Math.max(attempt - 1, 0), 8);
  return Math.min(200 * 2 ** shift, 30_000);
}

/**
 * Mirror of the Go tool registry (approvals.go): which tools exist and
 * the authority each may act under. message.send may only run through an
 * approved elevated call — a normal call is a structured block, never a
 * human prompt (ADR 0013 §2). `requiresApproval` mirrors Go's reserved
 * intrinsic-gating seam; no foundation tool uses it.
 */
// Exported for tests that register a phantom tool to probe edge paths
// (e.g. a granted call whose tool has no effect — Go toolAuthority parity).
export const TOOL_AUTHORITY: Record<
  string,
  { requiresApproval: boolean; elevatedOnly: boolean }
> = {
  "schedule.set": { requiresApproval: false, elevatedOnly: false },
  "journal.note": { requiresApproval: false, elevatedOnly: false },
  "job.start": { requiresApproval: false, elevatedOnly: false },
  "job.status": { requiresApproval: false, elevatedOnly: false },
  "job.cancel": { requiresApproval: false, elevatedOnly: false },
  "message.send": { requiresApproval: false, elevatedOnly: true },
};

// Go validates wake_at with time.RFC3339Nano — a bare date ("2026-09-14")
// or any non-RFC3339 shape is rejected even though new Date() would parse
// it. The double must be at least as strict (review f42).
const RFC3339_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

function parseRFC3339(v: unknown): Date | null {
  if (typeof v !== "string" || !RFC3339_RE.test(v)) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

function approvalRequirement(
  tool: string,
  route: string,
): "intrinsic" | "route" | "" {
  if (TOOL_AUTHORITY[tool]?.requiresApproval) return "intrinsic";
  if (route === "elevated") return "route";
  return "";
}

/**
 * Mirror of Go validateToolRequest: deterministic argument checks a gated
 * call must pass before a human is asked. Returns the error text, or null
 * when the call is well-formed.
 */
function validateToolRequest(
  tool: string,
  request: Record<string, unknown>,
): string | null {
  switch (tool) {
    case "schedule.set": {
      if (parseRFC3339(request.wake_at) === null) {
        return "bad request: schedule.set requires RFC3339 wake_at";
      }
      const missPolicy = (request.miss_policy as string) || "fire_late";
      if (!MISS_POLICIES.has(missPolicy)) {
        return "bad request: schedule.set miss_policy must be fire_late, coalesce, expire, or report_missed";
      }
      return null;
    }
    case "journal.note":
      return typeof request.text === "string" && request.text !== ""
        ? null
        : "bad request: journal.note requires text";
    case "message.send":
      return typeof request.text === "string" && request.text !== ""
        ? null
        : "bad request: message.send requires text";
    default:
      return null;
  }
}

/** Canonical JSON with sorted object keys — matches Go's json.Marshal maps. */
function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(",")}]`;
  const entries = Object.keys(v as Record<string, unknown>)
    .sort()
    .map(
      (k) =>
        `${JSON.stringify(k)}:${canonicalJson((v as Record<string, unknown>)[k])}`,
    );
  return `{${entries.join(",")}}`;
}

/**
 * Deterministic FNV-1a digest — the fake does not need real sha256; it
 * only needs a stable, distinct identity per (tool, route, request).
 */
function fakeDigest(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h.toString(16).padStart(8, "0");
}

function actionDigest(tool: string, route: string, request: Json): string {
  return fakeDigest(
    `sumi.core.tool-action.v1\x00${tool}\x00${route}\x00${canonicalJson(request)}`,
  );
}

function approvalId(persona: string, inputId: string, callIndex: number): string {
  return `appr-${fakeDigest(`${persona}\x00${inputId}\x00${callIndex}`)}`;
}

/**
 * In-memory StateClient implementing the same contract semantics as the Go
 * service — fencing, idempotent claims, atomic internal effects, recovery —
 * for fast unit tests. NOT canonical storage; real-PG verification lives in
 * the Go suite and the e2e harness.
 */
export class FakeState implements StateClient {
  personas = new Map<
    string,
    {
      human_id: string | null;
      display_name: string;
      created_at: string;
      authority: string;
      transfer_id: string | null;
    }
  >();
  /** Live or expired lease row per persona — release never deletes (Go B1 fix). */
  leases = new Map<string, WriterLease>();
  /** Monotonic fencing generation per persona; never recycled. */
  private generations = new Map<string, number>();
  inputs: Input[] = [];
  turns = new Map<string, Turn>();
  eventLog: Event[] = [];
  ops = new Map<string, Operation>(); // key: persona|tool|idem
  /** One durable plan per input — key: persona|input_id. Immutable. */
  plans = new Map<string, TurnPlan>();
  schedules = new Map<string, Schedule>();
  /** Persona-scoped execution records — key: persona|job_id. */
  jobs = new Map<string, Job>();
  outboxEntries: OutboxEntry[] = [];
  /** First commit request per turn — replay comparison (commit_request). */
  private commits = new Map<string, CommitRequest>();
  /** Durable approval records — key: approval_id. */
  approvals = new Map<string, Approval>();
  /** Test fixture: the model binding each persona resolves to. */
  modelBindings = new Map<string, ModelBinding>();
  private seq = 0;
  private outboxSeq = 0;

  private key(persona: string, tool: string, idem: string) {
    return `${persona}|${tool}|${idem}`;
  }

  private mustHold(persona: string, generation: number) {
    const lease = this.leases.get(persona);
    if (!lease || lease.generation !== generation) {
      throw new FencedError();
    }
  }

  addPersona(
    personaId: string,
    displayName = personaId,
    humanId: string | null = null,
  ) {
    this.personas.set(personaId, {
      human_id: humanId,
      display_name: displayName,
      created_at: new Date().toISOString(),
      authority: "active",
      transfer_id: null,
    });
  }

  addInput(personaId: string, inputId: string, text: string, kind = "message") {
    this.inputs.push({
      persona_id: personaId,
      input_id: inputId,
      kind,
      payload: { text },
      actor_kind: kind === "wake" ? "schedule" : "human",
      actor_id: "test",
      source_surface: "test",
      thread_id: "",
      occurred_at: null,
      attention: "reply",
      status: "queued",
      claimed_generation: null,
      turn_id: null,
      created_at: new Date().toISOString(),
      done_at: null,
      not_before: null,
      waiting_since: null,
      waited_ms: 0,
    });
  }

  async acquireWriter(
    persona: string,
    holder: string,
    ttlMs: number,
  ): Promise<WriterLease> {
    const now = Date.now();
    const held = this.leases.get(persona);
    // Same contract as the Go upsert: an unexpired lease blocks other
    // holders, while the same holder re-acquires and bumps the generation.
    if (
      held &&
      new Date(held.expires_at).getTime() > now &&
      held.holder_id !== holder
    ) {
      throw new StateError(409, "writer lease held by another live holder");
    }
    const lease: WriterLease = {
      persona_id: persona,
      generation: (this.generations.get(persona) ?? 0) + 1,
      holder_id: holder,
      acquired_at: new Date(now).toISOString(),
      expires_at: new Date(now + ttlMs).toISOString(),
    };
    this.generations.set(persona, lease.generation);
    this.leases.set(persona, lease);
    return lease;
  }

  async renewWriter(
    persona: string,
    holder: string,
    generation: number,
    ttlMs: number,
  ): Promise<WriterLease> {
    const held = this.leases.get(persona);
    if (!held || held.generation !== generation || held.holder_id !== holder) {
      throw new FencedError();
    }
    const lease = {
      ...held,
      expires_at: new Date(Date.now() + ttlMs).toISOString(),
    };
    this.leases.set(persona, lease);
    return lease;
  }

  async releaseWriter(
    persona: string,
    holder: string,
    generation: number,
  ): Promise<void> {
    // Never delete the row: generations are fencing tokens and must stay
    // monotonic. Release = expire in place; the next acquire bumps the
    // generation even for the same holder.
    const held = this.leases.get(persona);
    if (!held || held.holder_id !== holder || held.generation !== generation) {
      throw new FencedError();
    }
    this.leases.set(persona, {
      ...held,
      expires_at: new Date(Date.now() - 1).toISOString(),
    });
  }

  async recover(persona: string, generation: number): Promise<RecoverResult> {
    this.mustHold(persona, generation);
    const interrupted: string[] = [];
    const requeued: string[] = [];
    for (const t of this.turns.values()) {
      if (t.persona_id === persona && t.status === "running") {
        t.status = "interrupted";
        t.finished_at = new Date().toISOString();
        interrupted.push(t.turn_id);
      }
    }
    for (const i of this.inputs) {
      if (i.persona_id === persona && i.status === "claimed") {
        i.status = "queued";
        i.claimed_generation = null;
        i.turn_id = null;
        requeued.push(i.input_id);
      }
    }
    for (const s of this.schedules.values()) {
      if (s.persona_id === persona && s.status === "claimed") {
        s.status = "pending";
        s.claimed_generation = null;
      }
    }
    return {
      interrupted_turns: interrupted,
      requeued_inputs: requeued,
      released_schedule_claims: [],
    };
  }

  async loadTurn(
    persona: string,
    generation: number,
    turnId: string,
    contextLimit: number,
  ): Promise<LoadResult> {
    this.mustHold(persona, generation);
    // A running turn under another generation must be recovered first; under
    // ours it is a lost load response — return the same turn (Go LoadTurn).
    const running = [...this.turns.values()].find(
      (t) => t.persona_id === persona && t.status === "running",
    );
    if (running) {
      if (running.generation !== generation) {
        throw new StateError(409, "conflicting turn state");
      }
      const input = this.inputs.find((i) => i.input_id === running.input_id);
      if (!input) throw new Error("running turn input missing");
      return {
        turn: running,
        input,
        context: this.eventLog
          .filter((e) => e.persona_id === persona)
          .slice(-contextLimit),
        plan: this.plans.get(`${persona}|${input.input_id}`) ?? null,
      };
    }
    const input = this.inputs.find(
      (i) =>
        i.persona_id === persona &&
        i.status === "queued" &&
        // Go claim filter: a retryable-failed input stays parked until
        // its backoff expires, so it cannot hot-loop or starve others.
        (i.not_before === null || Date.parse(i.not_before) <= Date.now()),
    );
    if (!input) return { turn: null, input: null, context: [], plan: null };
    input.status = "claimed";
    input.claimed_generation = generation;
    input.turn_id = turnId;
    input.not_before = null;
    const turn: Turn = {
      persona_id: persona,
      turn_id: turnId,
      input_id: input.input_id,
      generation,
      // Go: a turn parked behind an approval is not a spent attempt.
      attempt:
        [...this.turns.values()].filter(
          (t) => t.input_id === input.input_id && t.status !== "awaiting",
        ).length + 1,
      status: "running",
      started_at: new Date().toISOString(),
      finished_at: null,
      output: null,
      usage: null,
      error: null,
    };
    this.turns.set(turnId, turn);
    return {
      turn,
      input,
      context: this.eventLog
        .filter((e) => e.persona_id === persona)
        .slice(-contextLimit),
      plan: this.plans.get(`${persona}|${input.input_id}`) ?? null,
    };
  }

  /**
   * Persist one round of an input's decisions before that round's effects
   * run (F1). The input is derived from the running turn — the caller can
   * never mis-assert it. One plan per input, append-only rounds: resaving
   * a recorded round is idempotent only when identical, a different
   * decision at a recorded position or a skipped round is rejected 409.
   */
  async savePlan(
    persona: string,
    generation: number,
    req: {
      turnId: string;
      round: number;
      text: string;
      calls: PlanCall[];
      usage: Record<string, unknown>;
    },
  ): Promise<{ plan: TurnPlan; created: boolean }> {
    this.mustHold(persona, generation);
    const turn = this.turns.get(req.turnId);
    if (!turn || turn.persona_id !== persona) {
      throw new StateError(404, "turn not found");
    }
    if (turn.generation !== generation || turn.status !== "running") {
      throw new StateError(409, "conflicting turn state");
    }
    // Go rejects a decision containing NUL at SavePlan — jsonb cannot
    // store it, and retrying can never succeed.
    if (hasNul(req.text) || hasNul(req.calls) || hasNul(req.usage ?? {})) {
      throw new StateError(
        400,
        "decision contains a NUL byte jsonb cannot store",
      );
    }
    const decision = {
      text: req.text,
      calls: req.calls,
      usage: req.usage ?? {},
    };
    const key = `${persona}|${turn.input_id}`;
    const stored = this.plans.get(key);
    if (stored) {
      const existing = stored.plan[req.round];
      if (existing) {
        const same =
          existing.text === decision.text &&
          jsonEqual(existing.calls, decision.calls) &&
          jsonEqual(existing.usage, decision.usage);
        if (!same) throw new StateError(409, "conflicting stored plan round");
        return { plan: stored, created: false };
      }
      if (req.round !== stored.plan.length) {
        throw new StateError(409, "plan round skips recorded rounds");
      }
      stored.plan.push(decision);
      return { plan: stored, created: true };
    }
    if (req.round !== 0) {
      throw new StateError(409, "first plan round must be round 0");
    }
    const plan: TurnPlan = {
      persona_id: persona,
      input_id: turn.input_id,
      turn_id: req.turnId,
      generation,
      plan: [decision],
      created_at: new Date().toISOString(),
    };
    this.plans.set(key, plan);
    return { plan, created: true };
  }

  async commitTurn(
    persona: string,
    turnId: string,
    generation: number,
    req: CommitRequest,
  ): Promise<Turn> {
    this.mustHold(persona, generation);
    const turn = this.turns.get(turnId);
    if (!turn || turn.persona_id !== persona) throw new Error("unknown turn");
    if (turn.status !== "running") {
      // Replay: an identical commit returns the stored result; a divergent
      // one conflicts (Go commit_request comparison, migration 47+).
      const stored = this.commits.get(turnId);
      if (stored && !jsonEqual(stored, req)) {
        throw new StateError(409, "conflicting turn state");
      }
      return turn;
    }
    // Parity with Go: committed payloads land in jsonb/text — a NUL in
    // events, output, or usage is a deterministic 400 at the commit
    // boundary (Go dataErr), not a retryable 500. The stored error is
    // diagnostic: Go strips NUL from it, so do the same.
    if (hasNul(req.events) || hasNul(req.output) || hasNul(req.usage)) {
      throw new StateError(
        400,
        "commit contains a NUL byte jsonb cannot store",
      );
    }
    // The Go server rejects bodies over maxBody (1 MiB, "read body")
    // before decode — mirror that boundary so oversized-commit fallback
    // tiers are reachable in unit tests.
    if (new TextEncoder().encode(JSON.stringify(req)).length > 1 << 20) {
      throw new StateError(400, "read body");
    }
    req = {
      ...req,
      error:
        req.error === undefined ? req.error : req.error.replace(/\u0000/g, ""),
    };
    for (const ev of req.events) {
      this.eventLog.push({
        persona_id: persona,
        seq: ++this.seq,
        turn_id: turnId,
        kind: ev.kind,
        payload: ev.payload,
        created_at: new Date().toISOString(),
      });
    }
    const input = this.inputs.find((i) => i.input_id === turn.input_id);
    if (!input) throw new Error("turn input missing");
    if (req.outcome === "complete") {
      turn.status = "done";
      turn.output = req.output ?? null;
      turn.usage = req.usage ?? null;
      input.status = "done";
      input.done_at = new Date().toISOString();
      this.outboxEntries.push({
        persona_id: persona,
        seq: ++this.outboxSeq,
        kind: "turn_completed",
        payload: {
          turn_id: turnId,
          input_id: input.input_id,
          output: req.output,
        },
        created_at: new Date().toISOString(),
        delivered_at: null,
      });
    } else if (req.outcome === "await") {
      // Mirror of the Go await commit: the input waits only while an
      // approval is still pending; a decision that already landed requeues
      // it directly.
      turn.status = "awaiting";
      const pending = [...this.approvals.values()].filter(
        (a) => a.persona_id === persona && a.input_id === input.input_id &&
          a.status === "pending",
      );
      if (pending.length === 0) {
        input.status = "queued";
        input.claimed_generation = null;
        input.turn_id = null;
        input.not_before = null;
      } else {
        input.status = "waiting";
        input.waiting_since = new Date().toISOString();
        this.outboxEntries.push({
          persona_id: persona,
          seq: ++this.outboxSeq,
          kind: "approval_requested",
          payload: {
            turn_id: turnId,
            input_id: input.input_id,
            approvals: pending.map((a) => ({
              approval_id: a.approval_id,
              tool: a.tool,
              route: a.route,
              required_by: a.required_by,
              request: a.request,
              action_digest: a.action_digest,
            })),
          },
          created_at: new Date().toISOString(),
          delivered_at: null,
        });
      }
    } else {
      turn.status = "failed";
      turn.error = req.error ?? "failed";
      if (req.retryable) {
        input.status = "queued";
        input.claimed_generation = null;
        input.turn_id = null;
        // Go: not_before = now() + max(retryBackoff(attempt),
        // clamped retry_after_ms) — a bounded, per-attempt growing delay
        // that also honors provider-supplied pacing.
        const after = Math.min(Math.max(req.retry_after_ms ?? 0, 0), 120_000);
        input.not_before = new Date(
          Date.now() + Math.max(retryBackoffMs(turn.attempt), after),
        ).toISOString();
      } else {
        input.status = "done";
        input.done_at = new Date().toISOString();
        // Parity with Go: a terminal failure must be visible to the
        // requester — the failure itself is the reply.
        this.outboxEntries.push({
          persona_id: persona,
          seq: ++this.outboxSeq,
          kind: "turn_failed",
          payload: {
            turn_id: turnId,
            input_id: input.input_id,
            error: turn.error,
          },
          created_at: new Date().toISOString(),
          delivered_at: null,
        });
      }
    }
    turn.finished_at = new Date().toISOString();
    this.commits.set(turnId, req);
    return turn;
  }

  async events(
    persona: string,
    afterSeq: number,
    limit = 200,
  ): Promise<Event[]> {
    return this.eventLog
      .filter((e) => e.persona_id === persona && e.seq > afterSeq)
      .slice(0, limit);
  }

  async claimOperation(
    persona: string,
    generation: number,
    op: {
      operationId: string;
      turnId: string;
      tool: string;
      callIndex: number;
      request: Record<string, unknown>;
    },
  ): Promise<{ operation: Operation; approval: Approval | null; fresh: boolean }> {
    // Unregistered tools are rejected at the boundary (Go ErrUnknownTool →
    // 400), before the fence check — a dangling 'running' op is never
    // recorded for a tool no executor can finish.
    if (!(op.tool in TOOL_AUTHORITY)) {
      throw new StateError(400, `unknown tool: ${op.tool}`);
    }
    // A NUL in the request is a deterministic data error (jsonb cannot
    // store it) — checked before the fence/plan checks like Go.
    if (hasNul(op.request)) {
      throw new StateError(
        400,
        `${op.tool} request contains a NUL byte jsonb cannot store`,
      );
    }
    this.mustHold(persona, generation);
    // Operations are attributed to the live turn: it must exist and be
    // running under this generation (Go claim enforces the same).
    const turn = this.turns.get(op.turnId);
    if (!turn || turn.persona_id !== persona) {
      throw new StateError(404, "turn not found");
    }
    if (turn.generation !== generation || turn.status !== "running") {
      throw new StateError(409, "conflicting turn state");
    }
    // Plan binding (F1): no plan → no claims. The claim must equal the
    // recorded call at the flat position across all rounds — an off-plan
    // position, request, or tool can never start a fresh effect.
    const plan = this.plans.get(`${persona}|${turn.input_id}`);
    if (!plan) {
      throw new StateError(409, "no plan recorded for this input");
    }
    const planned = plan.plan.flatMap((r) => r.calls)[op.callIndex];
    if (
      !planned ||
      planned.tool !== op.tool ||
      !jsonEqual(planned.request, op.request)
    ) {
      throw new StateError(409, "claim diverges from recorded plan");
    }
    // The requirement is derived from the recorded call's route, never
    // asserted by the caller (Go claimGated).
    const requiredBy = approvalRequirement(op.tool, planned.route);
    // Effect identity is server-derived: input_id + call_index. A
    // caller-supplied key or a new operation_id can never mint a second
    // effect for a decided position.
    const idempotencyKey = `${turn.input_id}:tool:${op.callIndex}`;
    const k = this.key(persona, op.tool, idempotencyKey);
    const existing = this.ops.get(k);
    if (existing) {
      // A replayed key is idempotent only for the identical request —
      // returning the stored receipt for a different request would record
      // an effect that never ran (Go ErrTurnConflict).
      if (!jsonEqual(existing.request, op.request)) {
        throw new StateError(409, "conflicting turn state");
      }
      // A running op claimed by a fenced generation is reclaimed for
      // re-execution (external tools; internal tools can't stay running).
      if (
        existing.status === "running" &&
        existing.claimed_generation !== generation
      ) {
        existing.claimed_generation = generation;
        existing.turn_id = op.turnId;
        return { operation: existing, approval: null, fresh: true };
      }
      if (existing.status === "awaiting_approval") {
        return this.claimGated(persona, turn.input_id, op.callIndex, existing, false);
      }
      // A replayed job.* receipt carries the job's state now next to the
      // original result (Go withCurrentJobTx); the stored receipt stays.
      if (op.tool.startsWith("job.") && existing.status === "done") {
        return {
          operation: {
            ...existing,
            response: this.withCurrentJob(persona, existing.response),
          },
          approval: null,
          fresh: false,
        };
      }
      return {
        operation: existing,
        approval: this.approvalFor(persona, turn.input_id, op.callIndex),
        fresh: false,
      };
    }
    const operation: Operation = {
      persona_id: persona,
      operation_id: op.operationId,
      turn_id: op.turnId,
      tool: op.tool,
      idempotency_key: idempotencyKey,
      request: op.request,
      // Go inserts 'running' for non-gated calls and 'awaiting_approval'
      // for gated ones; the claim transaction then finalizes or rolls
      // the row back entirely.
      status: requiredBy ? "awaiting_approval" : "running",
      response: null,
      claimed_generation: generation,
      created_at: new Date().toISOString(),
      completed_at: null,
    };
    if (requiredBy) {
      // The op row exists before validation on the gated path, exactly
      // like Go: inserted awaiting_approval, then finalized failed when
      // the deterministic check rejects it — replay returns the failure.
      this.ops.set(k, operation);
      // Go F2: a call that can never execute is never asked of the
      // human — validate before the approval row exists so nothing parks
      // and no grant is stranded.
      const verr = validateToolRequest(op.tool, op.request);
      if (verr !== null) {
        operation.status = "failed";
        operation.completed_at = new Date().toISOString();
        operation.response = { error: verr };
        return { operation, approval: null, fresh: true };
      }
      // Park: record the durable pending approval before returning — no
      // effect ran. The await commit turns this into the waiting input.
      const appr = this.insertApproval(
        persona,
        turn.input_id,
        op.callIndex,
        operation,
        planned.route,
        requiredBy,
      );
      return { operation, approval: appr, fresh: true };
    }
    // Below here the claim either stores the finalized op or throws — Go
    // rolls the insert back on an execution error, so nothing is stored
    // before the outcome is decided (no phantom receipt, review f42).
    this.ops.set(k, operation);
    // Go deniedIdenticalCall: a normal-route call identical to one the
    // human denied for this input finalizes failed with that denial.
    const denied = [...this.approvals.values()].find(
      (a) =>
        a.persona_id === persona &&
        a.input_id === turn.input_id &&
        a.tool === op.tool &&
        a.status === "denied" &&
        jsonEqual(a.request, op.request),
    );
    if (denied) {
      operation.status = "failed";
      operation.completed_at = new Date().toISOString();
      operation.response = {
        error: "denied",
        denial: {
          approval_id: denied.approval_id,
          decision: denied.decision,
          decided_by_kind: denied.decided_by_kind,
          decided_by_id: denied.decided_by_id,
        },
      };
      return { operation, approval: denied, fresh: true };
    }
    // Go F1: a normal call never produces a human prompt (ADR 0013 §2).
    // An elevated-only tool records a durable structured block — replayed
    // identically — rather than being promoted to an approval or
    // re-routed.
    if (TOOL_AUTHORITY[op.tool]?.elevatedOnly && planned.route !== "elevated") {
      operation.status = "failed";
      operation.completed_at = new Date().toISOString();
      operation.response = {
        error: "blocked",
        blocked: "elevated_route_required",
        detail: `${op.tool} only runs as an elevated call the human approves; a normal call asks no one and is never re-routed`,
      };
      return { operation, approval: null, fresh: true };
    }
    try {
      this.applyInternal(persona, turn.input_id, op.callIndex, op.turnId, operation);
    } catch (e) {
      // Go's claim transaction rolls back on an execution error — the
      // row must not survive as a replayable receipt (review f42).
      this.ops.delete(k);
      throw e;
    }
    return { operation, approval: null, fresh: true };
  }

  private approvalFor(
    persona: string,
    inputId: string,
    callIndex: number,
  ): Approval | null {
    return this.approvals.get(approvalId(persona, inputId, callIndex)) ?? null;
  }

  private insertApproval(
    persona: string,
    inputId: string,
    callIndex: number,
    op: Operation,
    route: "normal" | "elevated",
    requiredBy: "intrinsic" | "route",
  ): Approval {
    const id = approvalId(persona, inputId, callIndex);
    const existing = this.approvals.get(id);
    if (existing) return existing;
    const appr: Approval = {
      approval_id: id,
      persona_id: persona,
      input_id: inputId,
      call_index: callIndex,
      operation_id: op.operation_id,
      turn_id: op.turn_id,
      tool: op.tool,
      route,
      required_by: requiredBy,
      request: op.request,
      action_digest: actionDigest(op.tool, route, op.request),
      status: "pending",
      decision: null,
      decision_id: null,
      decided_by_kind: null,
      decided_by_id: null,
      provenance: null,
      decided_at: null,
      consumed_at: null,
      created_at: new Date().toISOString(),
    };
    this.approvals.set(id, appr);
    return appr;
  }

  /**
   * Mirror of Go claimGated: the approval row decides what a parked claim
   * does — pending stays parked, approved consumes the one-shot grant and
   * applies the effect in the same step, denied replays the finalized
   * failed operation.
   */
  private claimGated(
    persona: string,
    inputId: string,
    callIndex: number,
    op: Operation,
    freshInsert: boolean,
  ): { operation: Operation; approval: Approval | null; fresh: boolean } {
    const appr = this.approvalFor(persona, inputId, callIndex);
    if (!appr) {
      throw new StateError(500, `approval record missing for ${op.operation_id}`);
    }
    if (appr.status === "pending") {
      return { operation: op, approval: appr, fresh: freshInsert };
    }
    if (appr.status === "denied" || appr.consumed_at !== null) {
      // Denied finalized the op failed at decision time; consumed means
      // the grant already ran once — replay the stored record either way.
      return { operation: op, approval: appr, fresh: false };
    }
    // approved, unconsumed: the one-shot grant is consumed and the effect
    // applied in the same step — exactly-once under the granted provenance.
    appr.consumed_at = new Date().toISOString();
    try {
      this.applyInternal(persona, inputId, callIndex, op.turn_id, op);
    } catch (e) {
      // Go F2: a deterministic failure at execution (e.g. a schedule_id
      // taken while the human decided) must not leave the grant
      // approved-unconsumed with the op parked forever — the grant is
      // spent and the operation records the honest failure. A transient
      // (non-400) error rolls the whole claim back in Go — restore the
      // unconsumed grant so the next claim retries it (review f38).
      if (e instanceof StateError && e.status === 400) {
        op.status = "failed";
        op.completed_at = new Date().toISOString();
        op.response = { error: e.message };
        return { operation: op, approval: appr, fresh: true };
      }
      appr.consumed_at = null;
      throw e;
    }
    return { operation: op, approval: appr, fresh: true };
  }

  /** Apply a state-internal tool's effect and finalize the operation. */
  private applyInternal(
    persona: string,
    inputId: string,
    callIndex: number,
    turnId: string,
    operation: Operation,
  ) {
    const op = operation.request;
    if (operation.tool === "schedule.set") {
      const missPolicy = (op.miss_policy as string) ?? "fire_late";
      if (!MISS_POLICIES.has(missPolicy)) {
        throw new StateError(
          400,
          "schedule.set miss_policy must be fire_late, coalesce, expire, or report_missed",
        );
      }
      const wakeAt = parseRFC3339(op.wake_at);
      if (wakeAt === null) {
        throw new StateError(400, "schedule.set requires RFC3339 wake_at");
      }
      const sid = (op.schedule_id as string) ?? `sch-${Date.now()}`;
      const existing = this.schedules.get(`${persona}|${sid}`);
      if (existing) {
        // Crash replay never reaches here — receipts are keyed by plan
        // position. An identical request over a pending row is a true
        // idempotent set; anything else must not report a wake that was
        // not created.
        const identical =
          new Date(existing.wake_at).getTime() === wakeAt.getTime() &&
          existing.miss_policy === missPolicy &&
          jsonEqual(
            existing.payload,
            (op.payload as Record<string, unknown>) ?? {},
          );
        if (!identical) {
          throw new StateError(
            400,
            `schedule.set schedule_id ${sid} already exists with different contents`,
          );
        }
        if (existing.status !== "pending") {
          throw new StateError(
            400,
            `schedule.set schedule_id ${sid} is already ${existing.status}`,
          );
        }
        operation.response = { schedule: existing };
      } else {
        const sch: Schedule = {
          persona_id: persona,
          schedule_id: sid,
          wake_at: wakeAt.toISOString(),
          payload: (op.payload as Record<string, unknown>) ?? {},
          miss_policy: missPolicy as Schedule["miss_policy"],
          status: "pending",
          claimed_generation: null,
          created_at: new Date().toISOString(),
          fired_at: null,
        };
        this.schedules.set(`${persona}|${sid}`, sch);
        operation.response = { schedule: sch };
      }
    } else if (operation.tool === "message.send") {
      // Mirror of Go: the effect is an outbox entry the delivery layer
      // reads — the durable record of the secretary's outward message.
      const text = op.text;
      if (typeof text !== "string" || text === "") {
        throw new StateError(400, "message.send requires text");
      }
      this.outboxEntries.push({
        persona_id: persona,
        seq: ++this.outboxSeq,
        kind: "secretary_message",
        payload: { text, turn_id: turnId },
        created_at: new Date().toISOString(),
        delivered_at: null,
      });
      operation.response = {
        seq: this.outboxSeq,
        kind: "secretary_message",
      };
    } else if (operation.tool === "job.start") {
      // The job row is minted inside this claim transaction with a
      // server-derived id (plan position), so a replayed claim can never
      // mint a second job and the model never chooses an id.
      validateSubprocessRequest(op);
      const jobId = `op:${inputId}:${callIndex}`;
      const job = this.insertJob(
        persona,
        jobId,
        "subprocess",
        op,
        `tool:${turnId}:${callIndex}`,
      );
      // Receipts are snapshots (Go stores jsonb), never the live job row.
      operation.response = { job: structuredClone(job) };
    } else if (operation.tool === "job.status") {
      const jobId = op.job_id;
      if (typeof jobId !== "string" || jobId === "") {
        throw new StateError(400, "job.status requires job_id");
      }
      // Internal-tool boundary: Go maps ErrJobNotFound to 400 here so a
      // missing job is a recorded tool error, not a transient retry.
      operation.response = {
        job: structuredClone(this.mustJob400(persona, jobId)),
      };
    } else if (operation.tool === "job.cancel") {
      const jobId = op.job_id;
      if (typeof jobId !== "string" || jobId === "") {
        throw new StateError(400, "job.cancel requires job_id");
      }
      operation.response = {
        job: structuredClone(this.cancelJobRow(persona, jobId, true)),
      };
    } else if (operation.tool === "journal.note") {
      const text = op.text;
      if (typeof text !== "string" || text === "") {
        throw new StateError(400, "journal.note requires text");
      }
      const ev: Event = {
        persona_id: persona,
        seq: ++this.seq,
        turn_id: turnId,
        kind: "note",
        payload: { text },
        created_at: new Date().toISOString(),
      };
      this.eventLog.push(ev);
      operation.response = { seq: ev.seq, kind: "note" };
    } else {
      // A registered tool with no effect case fails the claim outright —
      // Go rolls the insert back (no phantom row), so a later claim
      // reports the same deterministic error (review f44).
      throw new StateError(
        400,
        `${operation.tool} has no registered effect to run`,
      );
    }
    operation.status = "done";
    operation.completed_at = new Date().toISOString();
  }

  async listApprovals(persona: string, approvalId?: string): Promise<Approval[]> {
    const all = [...this.approvals.values()].filter(
      (a) => a.persona_id === persona,
    );
    if (approvalId !== undefined) {
      const found = all.find((a) => a.approval_id === approvalId);
      if (!found) throw new StateError(404, "approval not found");
      return [found];
    }
    return all.sort((a, b) => a.created_at.localeCompare(b.created_at));
  }

  /**
   * Mirror of Go ResolveApproval: a pending approval resolves exactly once
   * under the authenticated human's one-shot decision. Identical replays
   * (same decision_id) return the stored record; a divergent decision on a
   * resolved approval conflicts — a denial is never overwritten.
   */
  async resolveApproval(
    persona: string,
    approvalId: string,
    decision: ApprovalDecision,
  ): Promise<Approval> {
    if (decision.decision !== "approve_once" && decision.decision !== "deny_once") {
      throw new StateError(400, "decision must be approve_once or deny_once");
    }
    // Go F3: decision_id is the command's idempotent identity — an empty
    // one could never be replayed, so it is rejected before any mutation.
    if (!decision.decision_id) {
      throw new StateError(400, "decision_id required");
    }
    if (decision.decided_by_kind !== "human" || !decision.decided_by_id) {
      throw new StateError(
        400,
        "decided_by_kind 'human' and decided_by_id required",
      );
    }
    const rec = this.personas.get(persona);
    // Go: a persona whose authority moved takes no new decisions here,
    // and an approval is identity-scoped — no bound human, no decider.
    if (rec && rec.authority !== "active") {
      throw new StateError(
        409,
        `persona authority is ${rec.authority}; it takes no new approval decisions`,
      );
    }
    const owner = rec?.human_id ?? null;
    if (owner === null) {
      throw new StateError(
        403,
        "persona is not bound to a human; no one may decide",
      );
    }
    if (owner !== decision.decided_by_id) {
      throw new StateError(403, "only the persona's human may decide");
    }
    const appr = this.approvals.get(approvalId);
    if (!appr || appr.persona_id !== persona) {
      throw new StateError(404, "approval not found");
    }
    if (appr.status !== "pending") {
      const same =
        appr.decision === decision.decision &&
        appr.decision_id === decision.decision_id &&
        appr.decided_by_kind === decision.decided_by_kind &&
        appr.decided_by_id === decision.decided_by_id;
      if (!decision.decision_id || !same) {
        throw new StateError(
          409,
          `approval ${approvalId} already ${appr.status}`,
        );
      }
      return appr;
    }
    appr.status = decision.decision === "approve_once" ? "approved" : "denied";
    appr.decision = decision.decision;
    appr.decision_id = decision.decision_id;
    appr.decided_by_kind = decision.decided_by_kind;
    appr.decided_by_id = decision.decided_by_id;
    appr.provenance =
      decision.decision === "approve_once"
        ? "agent_own_with_human_consent"
        : null;
    appr.decided_at = new Date().toISOString();
    if (decision.decision === "deny_once") {
      // Finalize the parked operation failed — the next claim replays the
      // denial rather than executing or waiting again.
      for (const op of this.ops.values()) {
        if (
          op.persona_id === persona &&
          op.operation_id === appr.operation_id &&
          op.status === "awaiting_approval"
        ) {
          op.status = "failed";
          op.completed_at = new Date().toISOString();
          op.response = {
            error: "denied",
            denial: {
              approval_id: appr.approval_id,
              decision: decision.decision,
              decided_by_kind: decision.decided_by_kind,
              decided_by_id: decision.decided_by_id,
            },
          };
        }
      }
    }
    // Journal the decision on the parked turn's record.
    this.eventLog.push({
      persona_id: persona,
      seq: ++this.seq,
      turn_id: appr.turn_id,
      kind: "approval_decided",
      payload: {
        approval_id: appr.approval_id,
        input_id: appr.input_id,
        call_index: appr.call_index,
        tool: appr.tool,
        route: appr.route,
        decision: decision.decision,
        decided_by_kind: decision.decided_by_kind,
        decided_by_id: decision.decided_by_id,
      },
      created_at: new Date().toISOString(),
    });
    // Requeue the parked input for resumption.
    const input = this.inputs.find(
      (i) => i.persona_id === persona && i.input_id === appr.input_id,
    );
    if (input && input.status === "waiting") {
      input.status = "queued";
      input.claimed_generation = null;
      input.turn_id = null;
      input.not_before = null;
      // Go F4: the human's thinking time accumulates durably so the
      // provider retry budget counts only active processing time.
      if (input.waiting_since) {
        input.waited_ms += Date.now() - Date.parse(input.waiting_since);
        input.waiting_since = null;
      }
    }
    return appr;
  }

  /** Test fixture: set the binding the persona resolves to. */
  setModelBinding(persona: string, binding: ModelBinding) {
    this.modelBindings.set(persona, binding);
  }

  modelBinding(persona: string): Promise<ModelBinding> {
    return Promise.resolve(
      this.modelBindings.get(persona) ?? { selection: "unset" },
    );
  }

  async completeOperation(
    persona: string,
    operationId: string,
    generation: number,
    response: Record<string, unknown>,
    failed: boolean,
  ): Promise<Operation> {
    this.mustHold(persona, generation);
    for (const op of this.ops.values()) {
      if (op.persona_id === persona && op.operation_id === operationId) {
        // Go: only the claiming generation may finish a running op; an
        // already-final op replays its stored record.
        if (op.status !== "running") return op;
        if (op.claimed_generation !== generation) throw new FencedError();
        op.status = failed ? "failed" : "done";
        op.response = response;
        op.completed_at = new Date().toISOString();
        return op;
      }
    }
    throw new Error("unknown operation");
  }

  async dispatchSchedules(
    persona: string,
    generation: number,
    now = new Date(),
    limit = 16,
  ): Promise<Schedule[]> {
    this.mustHold(persona, generation);
    const fired: Schedule[] = [];
    for (const s of this.schedules.values()) {
      if (fired.length >= limit) break;
      if (s.persona_id !== persona || s.status !== "pending") continue;
      if (new Date(s.wake_at) > now) continue;
      const inputId = `sched:${s.schedule_id}`;
      if (
        !this.inputs.some(
          (i) => i.persona_id === persona && i.input_id === inputId,
        )
      ) {
        this.inputs.push({
          persona_id: persona,
          input_id: inputId,
          kind: "wake",
          payload: s.payload,
          actor_kind: "schedule",
          actor_id: s.schedule_id,
          source_surface: "core_schedules",
          thread_id: "",
          occurred_at: null,
          attention: "reply",
          status: "queued",
          claimed_generation: null,
          turn_id: null,
          created_at: new Date().toISOString(),
          done_at: null,
          not_before: null,
          waiting_since: null,
          waited_ms: 0,
        });
      }
      s.status = "fired";
      s.fired_at = now.toISOString();
      s.claimed_generation = generation;
      fired.push(s);
    }
    return fired;
  }

  async outbox(
    persona: string,
    afterSeq: number,
    limit = 200,
  ): Promise<OutboxEntry[]> {
    return this.outboxEntries
      .filter((o) => o.persona_id === persona && o.seq > afterSeq)
      .slice(0, limit);
  }

  async personaState(persona: string): Promise<PersonaState> {
    const p = this.personas.get(persona);
    if (!p) throw new Error("unknown persona");
    const running = [...this.turns.values()].find(
      (t) => t.persona_id === persona && t.status === "running",
    );
    return {
      persona: { persona_id: persona, ...p },
      lease: this.leases.get(persona) ?? null,
      queued_inputs: this.inputs.filter(
        (i) => i.persona_id === persona && i.status === "queued",
      ).length,
      waiting_inputs: this.inputs.filter(
        (i) => i.persona_id === persona && i.status === "waiting",
      ).length,
      running_turn: running?.turn_id ?? null,
      pending_approvals: [...this.approvals.values()].filter(
        (a) => a.persona_id === persona && a.status === "pending",
      ).length,
      pending_schedules: [...this.schedules.values()].filter(
        (s) => s.persona_id === persona && s.status === "pending",
      ).length,
      latest_event_seq: Math.max(
        0,
        ...this.eventLog
          .filter((e) => e.persona_id === persona)
          .map((e) => e.seq),
      ),
    };
  }

  // --- jobs (M09) ---------------------------------------------------------
  // Same contract as the Go store: runner-claim ownership (not the writer
  // generation), atomic terminal+notification, identical-replay semantics.

  private mustJob(persona: string, jobId: string): Job {
    const j = this.jobs.get(`${persona}|${jobId}`);
    if (!j) throw new StateError(404, "job not found");
    return j;
  }

  // Internal-tool boundary: inside a claim the Go store maps "job not
  // found" to 400 so a missing job_id is recorded as a tool error rather
  // than retried as transient. The public routes keep 404.
  private mustJob400(persona: string, jobId: string): Job {
    const j = this.jobs.get(`${persona}|${jobId}`);
    if (!j) throw new StateError(400, "job not found");
    return j;
  }

  private insertJob(
    persona: string,
    jobId: string,
    kind: string,
    request: Record<string, unknown>,
    createdBy: string,
  ): Job {
    const existing = this.jobs.get(`${persona}|${jobId}`);
    if (existing) {
      if (existing.kind !== kind || !jsonEqual(existing.request, request)) {
        throw new StateError(409, "job_id replay carries a different request");
      }
      return existing;
    }
    const job: Job = {
      persona_id: persona,
      job_id: jobId,
      kind,
      request,
      status: "queued",
      claimed_by: null,
      claim_expires_at: null,
      created_by: createdBy,
      created_at: new Date().toISOString(),
      started_at: null,
      finished_at: null,
      cancel_requested_at: null,
      result: null,
      error: null,
      notified_at: null,
    };
    this.jobs.set(`${persona}|${jobId}`, job);
    return job;
  }

  /** Queue the 'job:<id>' terminal notification input exactly once. */
  private notifyJobTerminal(job: Job) {
    const inputId = `job:${job.job_id}`;
    if (
      !this.inputs.some(
        (i) => i.persona_id === job.persona_id && i.input_id === inputId,
      )
    ) {
      const command = this.jobCommandSummary(job);
      const origin = this.jobOrigin(job);
      const payload: Record<string, unknown> = {
        job_id: job.job_id,
        kind: job.kind,
        status: job.status,
        text:
          `job ${job.job_id} (${job.kind}) ${job.status}` +
          (job.error ? `: ${job.error}` : "") +
          (command ? ` — command: ${command}` : "") +
          (origin
            ? ` — started by you for request ${origin.inputId}` +
              (origin.request ? `: ${JSON.stringify(origin.request)}` : "") +
              (origin.inProgress
                ? " (that request was not finished yet when this job ended)"
                : "")
            : ""),
      };
      if (command) payload.command = command;
      if (origin) {
        payload.origin_input_id = origin.inputId;
        payload.origin_request = origin.request;
        payload.origin_in_progress = origin.inProgress;
      }
      if (job.error) payload.error = job.error;
      const code = job.result?.exit_code;
      if (code !== undefined) payload.exit_code = code;
      this.inputs.push({
        persona_id: job.persona_id,
        input_id: inputId,
        kind: "job_completed",
        payload,
        actor_kind: "job",
        actor_id: job.job_id,
        source_surface: "core_jobs",
        thread_id: "",
        occurred_at: null,
        attention: "reply",
        status: "queued",
        claimed_generation: null,
        turn_id: null,
        created_at: new Date().toISOString(),
        done_at: null,
        not_before: null,
        waiting_since: null,
        waited_ms: 0,
      });
    }
    job.notified_at = new Date().toISOString();
  }

  private jobCommandSummary(job: Job): string {
    const cmd = job.request.command;
    if (!Array.isArray(cmd) || cmd.length === 0) return "";
    return boundCodePoints(cmd.map((c) => String(c)).join(" "), 120);
  }

  // A tool-minted job (op:<input_id>:<call_index>) resolves to the request
  // it was started for, as Go jobOriginTx does.
  private jobOrigin(
    job: Job,
  ): { inputId: string; request: string; inProgress: boolean } | null {
    if (!job.created_by.startsWith("tool:") || !job.job_id.startsWith("op:")) {
      return null;
    }
    const rest = job.job_id.slice(3);
    const i = rest.lastIndexOf(":");
    if (i <= 0) return null;
    const input = this.inputs.find(
      (x) => x.persona_id === job.persona_id && x.input_id === rest.slice(0, i),
    );
    if (!input) return null;
    const text =
      typeof input.payload.text === "string" ? input.payload.text : "";
    return {
      inputId: input.input_id,
      request: boundCodePoints(text, 200),
      inProgress: input.status !== "done",
    };
  }

  private withCurrentJob(
    persona: string,
    receipt: Operation["response"],
  ): Operation["response"] {
    const r = receipt as Record<string, unknown> | null;
    const jobId = (r?.job as Job | undefined)?.job_id;
    const j = jobId ? this.jobs.get(`${persona}|${jobId}`) : undefined;
    if (!r || !j) return receipt;
    const current: Record<string, unknown> = { status: j.status };
    if (j.result?.exit_code !== undefined)
      current.exit_code = j.result.exit_code;
    if (j.error) current.error = j.error;
    if (j.finished_at) current.finished_at = j.finished_at;
    return {
      ...r,
      current_job: current,
      receipt_note:
        "job is this call's original result; current_job is the job's state when this turn resumed",
    } as Operation["response"];
  }

  private cancelJobRow(persona: string, jobId: string, internal = false): Job {
    const job = internal
      ? this.mustJob400(persona, jobId)
      : this.mustJob(persona, jobId);
    if (job.status === "queued") {
      job.status = "cancelled";
      job.cancel_requested_at = new Date().toISOString();
      job.finished_at = job.cancel_requested_at;
      this.notifyJobTerminal(job);
    } else if (job.status === "running") {
      job.status = "cancel_requested";
      job.cancel_requested_at = new Date().toISOString();
    }
    return job;
  }

  async submitJob(
    persona: string,
    job: { jobId: string; kind: string; request: Record<string, unknown> },
  ): Promise<{ job: Job; created: boolean }> {
    if (!job.jobId || job.jobId.length > 256) {
      throw new StateError(400, "job_id must be 1-256 characters");
    }
    if (job.jobId.startsWith("op:") || job.jobId === "claim") {
      throw new StateError(400, `job_id ${job.jobId} is reserved`);
    }
    if (hasNul(job.jobId)) {
      throw new StateError(400, "job_id contains a NUL byte text cannot store");
    }
    if (job.kind !== "subprocess") {
      throw new StateError(400, `unknown job kind ${job.kind}`);
    }
    validateSubprocessRequest(job.request);
    if (hasNul(job.request)) {
      throw new StateError(
        400,
        "job request contains a NUL byte jsonb cannot store",
      );
    }
    const key = `${persona}|${job.jobId}`;
    const existed = this.jobs.has(key);
    const stored = this.insertJob(
      persona,
      job.jobId,
      job.kind,
      job.request,
      "api",
    );
    return { job: stored, created: !existed };
  }

  async getJob(persona: string, jobId: string): Promise<Job> {
    return this.mustJob(persona, jobId);
  }

  async listJobs(
    persona: string,
    opts?: { status?: Job["status"][]; limit?: number },
  ): Promise<Job[]> {
    return [...this.jobs.values()]
      .filter(
        (j) =>
          j.persona_id === persona &&
          (!opts?.status?.length || opts.status.includes(j.status)),
      )
      .sort((a, b) => b.created_at.localeCompare(a.created_at))
      .slice(0, opts?.limit ?? 50);
  }

  async cancelJob(persona: string, jobId: string): Promise<Job> {
    return this.cancelJobRow(persona, jobId);
  }

  async claimJobs(
    persona: string,
    req: { runnerId: string; kinds: string[]; leaseMs: number; limit?: number },
  ): Promise<{ claimed: Job[]; swept: Job[] }> {
    const now = Date.now();
    const swept: Job[] = [];
    for (const j of this.jobs.values()) {
      if (
        j.persona_id === persona &&
        (j.status === "running" || j.status === "cancel_requested") &&
        j.claim_expires_at !== null &&
        Date.parse(j.claim_expires_at) < now
      ) {
        j.status = "lost";
        j.finished_at = new Date().toISOString();
        j.error = "runner claim expired; outcome is indeterminate";
        j.result = { ...(j.result ?? {}), reason: "claim_expired" };
        this.notifyJobTerminal(j);
        swept.push(j);
      }
    }
    const claimed: Job[] = [];
    const limit = req.limit ?? 1;
    for (const j of [...this.jobs.values()].sort((a, b) =>
      a.created_at.localeCompare(b.created_at),
    )) {
      if (claimed.length >= limit) break;
      if (
        j.persona_id !== persona ||
        j.status !== "queued" ||
        !req.kinds.includes(j.kind)
      ) {
        continue;
      }
      j.status = "running";
      j.claimed_by = req.runnerId;
      j.claim_expires_at = new Date(now + req.leaseMs).toISOString();
      j.started_at ??= new Date().toISOString();
      claimed.push(j);
    }
    return { claimed, swept };
  }

  async heartbeatJob(
    persona: string,
    jobId: string,
    req: { runnerId: string; leaseMs: number },
  ): Promise<Job> {
    const job = this.mustJob(persona, jobId);
    if (job.claimed_by !== req.runnerId || JOB_TERMINAL.has(job.status)) {
      throw new StateError(409, "job is not claimed by this runner", job);
    }
    job.claim_expires_at = new Date(Date.now() + req.leaseMs).toISOString();
    return job;
  }

  async completeJob(
    persona: string,
    jobId: string,
    req: {
      runnerId: string;
      status: JobTerminalReport;
      result: Record<string, unknown>;
      error?: string;
    },
  ): Promise<Job> {
    if (!["done", "failed", "cancelled"].includes(req.status)) {
      throw new StateError(
        400,
        "complete status must be done, failed, or cancelled",
      );
    }
    if (hasNul(req.result)) {
      throw new StateError(
        400,
        "job result contains a NUL byte jsonb cannot store",
      );
    }
    if (hasNul(req.error ?? "")) {
      throw new StateError(
        400,
        "job error contains a NUL byte text cannot store",
      );
    }
    const job = this.mustJob(persona, jobId);
    if (JOB_TERMINAL.has(job.status)) {
      const same =
        job.status === req.status &&
        jsonEqual(job.result ?? {}, req.result) &&
        (job.error ?? "") === (req.error ?? "");
      if (!same) {
        throw new StateError(409, `job already finished as ${job.status}`, job);
      }
      return job;
    }
    if (job.claimed_by !== req.runnerId) {
      throw new StateError(409, "job is not claimed by this runner", job);
    }
    job.status = req.status;
    job.result = req.result;
    job.error = req.error || null;
    job.finished_at = new Date().toISOString();
    this.notifyJobTerminal(job);
    return job;
  }
}

/** Cut to at most n code points, marking the cut (Go boundRunes). */
function boundCodePoints(s: string, n: number): string {
  const cps = [...s];
  return cps.length > n ? `${cps.slice(0, n).join("")}…` : s;
}
