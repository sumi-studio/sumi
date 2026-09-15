import { jsonEqual } from "./json.ts";
import { FencedError, type StateClient, StateError } from "./state-client.ts";
import type {
  Approval,
  ApprovalDecision,
  ClaimedMemoryChunk,
  CommitRequest,
  Event,
  FundingRef,
  Input,
  Job,
  JobTerminalReport,
  Json,
  LoadResult,
  MemoryBlock,
  MemoryChunk,
  MemoryStatus,
  ModelBinding,
  OmittedMemory,
  Operation,
  OutboxEntry,
  PersonaState,
  PlanCall,
  RecoverResult,
  RenderedContext,
  Schedule,
  Turn,
  TurnPlan,
  UsageAdmitResult,
  UsageEstimate,
  UsageFact,
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

// Memory thresholds mirrored from the Go store (agentstate/memory.go).
const L0_CHUNK_MIN_TOKENS = 10_000;
/** Target bound for one sealed chunk: past it, any safe boundary may cut. */
const L0_FORCED_SEAL_LIMIT_TOKENS = L0_CHUNK_MIN_TOKENS * 2;
const L0_LIVE_LIMIT_TOKENS = 40_000;
/** Recorded preparation failures a chunk may spend. */
const MEMORY_CHUNK_MAX_ATTEMPTS = 3;
/** Claims ending without a recorded outcome before a chunk is marked failed. */
const MEMORY_CHUNK_MAX_INTERRUPTIONS = 8;
/** Go memoryReshelvePacing: shelf delay after an unavailable model layer. */
const MEMORY_RESHELVE_PACING_MS = 200;
/** Estimated tokens of applied memory blocks admitted into one context. */
const MEMORY_SEND_CAP_TOKENS = 25_000;
/** Applied L1 beyond this triggers an L1→L2 consolidation target. */
const L1_LIMIT_TOKENS = 15_000;
/** One L1→L2 target consumes until at most this much applied L1 remains. */
const L1_DROP_TO_TOKENS = 11_000;
/** Applied L2 beyond this triggers L2-internal reintegration. */
const L2_LIMIT_TOKENS = 10_000;
/** Journal records one conversation_history search call scans. */
const HISTORY_SEARCH_SCAN_RECORDS = 2_000;
const HISTORY_READ_CHAR_BUDGET = 16 * 1024;
const L0_SEND_CAP_TOKENS = 60_000;
const CONTEXT_MAX_EVENTS = 5_000;

/** Matches Go estPayloadTokens: ~4 bytes/token over stored JSON + overhead. */
function estEventTokens(
  kind: string,
  payload: Record<string, unknown>,
): number {
  return Math.ceil((kind.length + 16 + JSON.stringify(payload).length) / 4);
}
function estTextTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

/**
 * journal_event_v1: the serialization read returns and search matches —
 * sorted object keys and no added spaces, like Go's map encoding with HTML
 * escaping off.
 */
function journalEventJson(e: Event): string {
  return JSON.stringify(
    {
      seq: e.seq,
      turn_id: e.turn_id,
      kind: e.kind,
      created_at: e.created_at,
      payload: e.payload,
    },
    (_k, v: unknown) =>
      v !== null && typeof v === "object" && !Array.isArray(v)
        ? Object.fromEntries(
            Object.keys(v)
              .sort()
              .map((k) => [k, (v as Record<string, unknown>)[k]]),
          )
        : v,
  );
}

/** Go admitApplied: newest blocks first while they fit the memory cap; the
 * older remainder is left out as one explicit range. */
function admitApplied(
  blocks: MemoryBlock[],
): [MemoryBlock[], OmittedMemory | null] {
  let used = 0;
  let cut = blocks.length;
  for (let i = blocks.length - 1; i >= 0; i--) {
    const b = blocks[i] as MemoryBlock;
    if (used + b.est_tokens > MEMORY_SEND_CAP_TOKENS) break;
    used += b.est_tokens;
    cut = i;
  }
  if (cut === 0) return [blocks, null];
  const older = blocks.slice(0, cut);
  const first = older[0] as MemoryBlock;
  const last = older[older.length - 1] as MemoryBlock;
  return [
    blocks.slice(cut),
    {
      count: older.length,
      first_chunk_seq: first.chunk_seq,
      last_chunk_seq: last.chunk_seq,
      first_seq: first.first_seq,
      last_seq: last.last_seq,
      first_time: first.first_time,
      last_time: last.last_time,
      est_tokens: older.reduce((n, b) => n + b.est_tokens, 0),
    },
  ];
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
  conversation_history: { requiresApproval: false, elevatedOnly: false },
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

function approvalId(
  persona: string,
  inputId: string,
  callIndex: number,
): string {
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
      /** Carried non-secret model intent (Go core_personas.model_intent). */
      model_intent: {
        kind: string;
        connection?: Record<string, unknown>;
      } | null;
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
  /** Sealed journal ranges and their L1 replacement lifecycle. */
  memoryChunks: MemoryChunk[] = [];
  /** First commit request per turn — replay comparison (commit_request). */
  private commits = new Map<string, CommitRequest>();
  /** Durable approval records — key: approval_id. */
  approvals = new Map<string, Approval>();
  /** Test fixture: the model binding each persona resolves to. */
  modelBindings = new Map<string, ModelBinding>();
  /** Configured budgets — key: funding_kind|funding_id (usage_budgets). */
  usageBudgets = new Map<
    string,
    {
      limit_minor: number;
      currency: string;
      rate_input_per_mtok: number;
      rate_output_per_mtok: number;
      rate_cached_per_mtok: number | null;
      pricing_revision: string;
    }
  >();
  /** Admission holds — key: persona|fact_id (usage_reservations). The row
   *  snapshots the call's identity and the rate card that priced it, so a
   *  lost record reconciles into an 'unrecorded' fact and a mid-call rate
   *  edit cannot rewrite that call's cost basis (Go parity). */
  usageReservations = new Map<
    string,
    {
      fact_id: string;
      kind: string;
      phase: string;
      turn_id: string | null;
      input_id: string | null;
      round: number;
      funding_kind: string;
      funding_id: string;
      funding: FundingRef;
      reserved_minor: number;
      currency: string | null;
      bounded: boolean;
      est_input_tokens: number | null;
      est_output_bound: number | null;
      rate_input_per_mtok: number | null;
      rate_output_per_mtok: number | null;
      rate_cached_per_mtok: number | null;
      pricing_revision: string | null;
      generation: number;
      status: "held" | "settled" | "released";
    }
  >();
  /** The usage ledger — key: persona|fact_id (usage_facts). */
  usageFacts = new Map<string, UsageFact>();
  /** Parked budget waits — key: persona|input_id (core_budget_waits). */
  budgetWaits = new Map<
    string,
    {
      persona_id: string;
      input_id: string;
      turn_id: string;
      funding_kind: string;
      funding_id: string;
      needed_minor: number;
      currency: string;
      created_at: string;
    }
  >();
  /** Seq of each input's one input_received event (core_inputs.received_seq). */
  private receivedSeq = new Map<string, number>();
  /** Per-persona seqs — Go allocates MAX(seq)+1 per persona for both
   *  core_events and core_outbox, so a second persona starts at 1. */
  private seq = new Map<string, number>();
  private outboxSeq = new Map<string, number>();
  /** Registered delegated effects — Go Store.RegisterEffect parity. A tool
   *  here is claimable; its applier runs where Go would run Apply in-tx. */
  private registeredEffects = new Map<
    string,
    (persona: string, idemKey: string, request: Record<string, unknown>) => Json
  >();

  private key(persona: string, tool: string, idem: string) {
    return `${persona}|${tool}|${idem}`;
  }

  private nextSeq(map: Map<string, number>, persona: string) {
    const next = (map.get(persona) ?? 0) + 1;
    map.set(persona, next);
    return next;
  }

  /** Go allocates MAX(chunk_seq)+1 per persona, so rows seeded directly —
   *  or carried in a future transfer — still get the next free seq. */
  private nextChunkSeq(persona: string) {
    return (
      Math.max(
        0,
        ...this.memoryChunks
          .filter((c) => c.persona_id === persona)
          .map((c) => c.chunk_seq),
      ) + 1
    );
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
      model_intent: null,
    });
  }

  /** Test fixture: set the persona's authority state (active|staged|sealed|transferred|retired). */
  setPersonaAuthority(personaId: string, authority: string) {
    const rec = this.personas.get(personaId);
    if (!rec) throw new StateError(404, "persona not found");
    rec.authority = authority;
  }

  /** Test fixture: set the carried non-secret model intent. */
  setModelIntent(
    personaId: string,
    intent: { kind: string; connection?: Record<string, unknown> } | null,
  ) {
    const rec = this.personas.get(personaId);
    if (!rec) throw new StateError(404, "persona not found");
    rec.model_intent = intent;
  }

  /** Mirror of the Go clear route: the intent is part of a sealed cut, so
   * clearing is fenced to staged/active personas — a sealed or transferred
   * one refuses (409) like the real store. */
  clearModelIntent(personaId: string) {
    const rec = this.personas.get(personaId);
    if (!rec) throw new StateError(404, "persona not found");
    if (rec.authority !== "staged" && rec.authority !== "active") {
      throw new StateError(
        409,
        `persona is not active in this placement: persona authority is ${rec.authority}`,
      );
    }
    rec.model_intent = null;
  }

  /** Mirror of the Go bind route: an unbound staged/active persona binds; a bound or moved one refuses. */
  bindHuman(personaId: string, humanId: string) {
    const rec = this.personas.get(personaId);
    if (!rec) throw new StateError(404, "persona not found");
    if (rec.human_id !== null) {
      // Same-human retry is idempotent; a different one conflicts.
      if (rec.human_id === humanId) return;
      throw new StateError(
        409,
        `persona is already bound to a different human: bound to ${rec.human_id}`,
      );
    }
    if (rec.authority !== "staged" && rec.authority !== "active") {
      throw new StateError(
        409,
        `persona is not active in this placement: persona authority is ${rec.authority}`,
      );
    }
    rec.human_id = humanId;
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
    // Go fences on authority before the lease check: a sealed, staged,
    // transferred or retired persona may not acquire a writer here, and a
    // missing persona is a 404 — not a fresh lease on a ghost.
    const rec = this.personas.get(persona);
    if (!rec) {
      throw new StateError(404, "persona not found");
    }
    if (rec.authority !== "active") {
      throw new StateError(
        409,
        `persona is not active in this placement: authority is ${rec.authority}`,
      );
    }
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
    // Memory chunks a fenced generation was preparing count an interruption.
    this.interruptPreparing(persona, generation);
    // Held reservations from dead generations reconcile the same way Go
    // Recover does: the fact landed → settled; it never did → an
    // inspectable 'unrecorded' fact carrying the estimate, not a release.
    for (const [k, r] of this.usageReservations) {
      if (
        k.startsWith(`${persona}|`) &&
        r.generation < generation &&
        r.status === "held"
      ) {
        this.reconcileHeldReservation(k, r);
      }
    }
    return {
      interrupted_turns: interrupted,
      requeued_inputs: requeued,
      released_schedule_claims: [],
    };
  }

  /**
   * Test fixture: install or replace a budget on a funding source, then
   * resume waits the new cap can now admit (Go SetBudget parity — a cap
   * without a rate card refuses, since it cannot bound spend).
   */
  setUsageBudget(
    kind: string,
    id: string,
    budget: {
      limit_minor: number;
      currency: string;
      rate_input_per_mtok: number;
      rate_output_per_mtok: number;
      rate_cached_per_mtok?: number | null;
      pricing_revision?: string;
    },
  ) {
    this.usageBudgets.set(`${kind}|${id}`, {
      limit_minor: budget.limit_minor,
      currency: budget.currency,
      rate_input_per_mtok: budget.rate_input_per_mtok,
      rate_output_per_mtok: budget.rate_output_per_mtok,
      rate_cached_per_mtok: budget.rate_cached_per_mtok ?? null,
      pricing_revision: budget.pricing_revision ?? "",
    });
    this.resumeBudgetWaits(kind, id);
  }

  /** Test fixture: remove the cap — uncapped is explicit — and resume
   * every input waiting on that funding (Go ClearBudget parity). */
  clearUsageBudget(kind: string, id: string) {
    this.usageBudgets.delete(`${kind}|${id}`);
    this.resumeBudgetWaits(kind, id);
  }

  /** Test fixture: a selection/connection change re-resolves funding, so
   * every budget-parked input of the human's personas resumes
   * (Go ResumeWaitsForHuman parity). */
  resumeBudgetWaitsForHuman(humanId: string) {
    for (const [key, w] of [...this.budgetWaits]) {
      const p = this.personas.get(w.persona_id);
      if (p?.human_id !== humanId) continue;
      this.budgetWaits.delete(key);
      this.requeueWaitingInput(w.persona_id, w.input_id);
    }
    for (const c of this.memoryChunks) {
      const p = this.personas.get(c.persona_id);
      if (
        p?.human_id === humanId &&
        c.status === "sealed" &&
        c.not_before !== null &&
        (c.last_error ?? "").startsWith("budget-wait:")
      ) {
        c.not_before = null;
      }
    }
  }

  /**
   * Go resumeWaits: delete the matching wait rows whose needed amount now
   * fits (or all of them when the cap is gone), requeue still-waiting
   * inputs — unless a pending approval is a second, independent wait —
   * and unpark memory chunks reshelved on a budget wait.
   */
  private resumeBudgetWaits(kind: string, id: string) {
    const budget = this.usageBudgets.get(`${kind}|${id}`);
    let fitsNeeded = -1;
    if (budget) {
      const { spent, held } = this.fundingSpend(kind, id, budget.currency);
      fitsNeeded = budget.limit_minor - spent - held;
    }
    for (const [key, w] of [...this.budgetWaits]) {
      if (w.funding_kind !== kind || w.funding_id !== id) continue;
      if (fitsNeeded >= 0 && w.needed_minor > fitsNeeded) continue;
      this.budgetWaits.delete(key);
      this.requeueWaitingInput(w.persona_id, w.input_id);
    }
    // Chunks reshelved on a budget wait for this funding belong to its
    // owner's personas; the fake has no funding→owner table, so unpark
    // every budget-wait-paced chunk whose persona could reach it — a
    // wider wake than Go's owner-scoped one, harmless in tests.
    for (const c of this.memoryChunks) {
      if (
        c.status === "sealed" &&
        c.not_before !== null &&
        (c.last_error ?? "").startsWith("budget-wait:")
      ) {
        c.not_before = null;
      }
    }
  }

  /**
   * Requeue a still-waiting input, accumulating its parked time into
   * waited_ms like the Go resume path — but never one a pending approval
   * still holds (a second, independent wait).
   */
  private requeueWaitingInput(persona: string, inputId: string) {
    const input = this.inputs.find(
      (i) =>
        i.persona_id === persona &&
        i.input_id === inputId &&
        i.status === "waiting",
    );
    if (!input) return;
    const pendingApproval = [...this.approvals.values()].some(
      (a) =>
        a.persona_id === persona &&
        a.input_id === inputId &&
        a.status === "pending",
    );
    if (pendingApproval) return;
    if (input.waiting_since) {
      input.waited_ms += Date.now() - Date.parse(input.waiting_since);
      input.waiting_since = null;
    }
    input.status = "queued";
    input.claimed_generation = null;
    input.turn_id = null;
    input.not_before = null;
  }

  /** Go fundingSpend: currency-dimensioned — spend in one currency never
   *  counts toward a cap denominated in another. */
  private fundingSpend(kind: string, id: string, currency: string) {
    let spent = 0;
    let held = 0;
    for (const f of this.usageFacts.values()) {
      if (
        f.funding.kind === kind &&
        f.funding.id === id &&
        f.currency === currency &&
        f.cost_minor
      ) {
        spent += f.cost_minor;
      }
    }
    for (const r of this.usageReservations.values()) {
      if (
        r.funding_kind === kind &&
        r.funding_id === id &&
        r.currency === currency &&
        r.status === "held"
      ) {
        held += r.reserved_minor;
      }
    }
    return { spent, held };
  }

  async admitUsage(
    persona: string,
    generation: number,
    req: {
      factId: string;
      kind: string;
      phase: string;
      turnId?: string;
      inputId?: string;
      round?: number;
      funding: FundingRef;
      estimate: UsageEstimate;
    },
  ): Promise<UsageAdmitResult> {
    // Admission is writer-fenced (Go requireGeneration): a dead writer
    // cannot reserve new spend.
    this.mustHold(persona, generation);
    if (!req.factId || !req.kind || !req.phase) {
      throw new StateError(400, "fact_id, kind and phase are required");
    }
    if (
      req.funding.kind !== "connection" &&
      req.funding.kind !== "operator" &&
      req.funding.kind !== "sumi"
    ) {
      throw new StateError(400, `unknown funding kind ${req.funding.kind}`);
    }
    const key = `${persona}|${req.factId}`;
    const existing = this.usageReservations.get(key);
    if (existing) {
      if (
        existing.funding_kind !== req.funding.kind ||
        existing.funding_id !== req.funding.id
      ) {
        throw new StateError(
          409,
          `fact_id ${req.factId} already admitted under different funding`,
        );
      }
      if (existing.status !== "held") {
        throw new StateError(
          409,
          `fact_id ${req.factId} already ${existing.status}`,
        );
      }
      return {
        admitted: true,
        reservation: {
          fact_id: existing.fact_id,
          reserved_minor: existing.reserved_minor,
          currency: existing.currency ?? undefined,
          bounded: existing.bounded,
          status: existing.status,
        },
      };
    }
    const budget = this.usageBudgets.get(
      `${req.funding.kind}|${req.funding.id}`,
    );
    let needed = 0;
    let bounded = req.estimate.output_tokens_bound !== undefined;
    if (budget) {
      const price = (tok: number, rate: number) =>
        tok <= 0 || rate <= 0 ? 0 : Math.ceil((tok * rate) / 1_000_000);
      needed =
        price(req.estimate.input_tokens, budget.rate_input_per_mtok) +
        (req.estimate.output_tokens_bound !== undefined
          ? price(
              req.estimate.output_tokens_bound,
              budget.rate_output_per_mtok,
            )
          : 0);
      const { spent, held } = this.fundingSpend(
        req.funding.kind,
        req.funding.id,
        budget.currency,
      );
      if (needed > budget.limit_minor || spent + held > budget.limit_minor - needed) {
        return {
          admitted: false,
          wait: {
            funding: req.funding,
            needed_minor: needed,
            limit_minor: budget.limit_minor,
            spent_minor: spent,
            held_minor: held,
            remaining_minor: budget.limit_minor - spent - held,
            currency: budget.currency,
            pricing_revision: budget.pricing_revision,
            bounded,
          },
        };
      }
    }
    // Snapshot the rate card that priced this admission — the fact is
    // priced under it even if the budget is edited or removed mid-call.
    this.usageReservations.set(key, {
      fact_id: req.factId,
      kind: req.kind,
      phase: req.phase,
      turn_id: req.turnId ?? null,
      input_id: req.inputId ?? null,
      round: req.round ?? 0,
      funding_kind: req.funding.kind,
      funding_id: req.funding.id,
      funding: req.funding,
      reserved_minor: needed,
      currency: budget?.currency ?? null,
      bounded,
      est_input_tokens: req.estimate.input_tokens,
      est_output_bound: req.estimate.output_tokens_bound ?? null,
      rate_input_per_mtok: budget?.rate_input_per_mtok ?? null,
      rate_output_per_mtok: budget?.rate_output_per_mtok ?? null,
      rate_cached_per_mtok: budget?.rate_cached_per_mtok ?? null,
      pricing_revision: budget?.pricing_revision ?? null,
      generation,
      status: "held",
    });
    return {
      admitted: true,
      reservation: {
        fact_id: req.factId,
        reserved_minor: needed,
        currency: budget?.currency,
        bounded,
        status: "held",
      },
    };
  }

  async recordUsage(
    persona: string,
    req: {
      factId: string;
      kind: string;
      phase: string;
      turnId?: string;
      inputId?: string;
      round?: number;
      funding: FundingRef;
      status: "reported" | "unknown" | "not_sent";
      inputTokens?: number | null;
      outputTokens?: number | null;
      cachedTokens?: number | null;
      quantities?: Record<string, unknown>;
    },
  ): Promise<{ fact: UsageFact; created: boolean }> {
    // Not writer-fenced — the spend already happened (Go parity).
    if (!req.factId || !req.kind || !req.phase) {
      throw new StateError(400, "fact_id, kind and phase are required");
    }
    if (
      req.status !== "reported" &&
      req.status !== "unknown" &&
      req.status !== "not_sent"
    ) {
      throw new StateError(
        400,
        "status must be reported, unknown or not_sent",
      );
    }
    const key = `${persona}|${req.factId}`;
    const res = this.usageReservations.get(key);

    // The call's rate card: the admission snapshot when it was admitted
    // under a configured budget (an edited or removed budget cannot
    // rewrite this call's cost basis); else the current budget.
    const card =
      res?.currency != null &&
      res.rate_input_per_mtok != null &&
      res.rate_output_per_mtok != null
        ? {
            currency: res.currency,
            rate_input_per_mtok: res.rate_input_per_mtok,
            rate_output_per_mtok: res.rate_output_per_mtok,
            rate_cached_per_mtok: res.rate_cached_per_mtok,
            pricing_revision: res.pricing_revision ?? "",
          }
        : (this.usageBudgets.get(`${req.funding.kind}|${req.funding.id}`) ??
          null);

    const price = (tok: number, rate: number) =>
      tok <= 0 || rate <= 0 ? 0 : Math.ceil((tok * rate) / 1_000_000);
    let costMinor: number | null = null;
    let currency: string | undefined;
    let costBasis: string | undefined;
    let revision: string | undefined;
    if (
      req.status === "reported" &&
      card &&
      req.inputTokens != null &&
      req.outputTokens != null
    ) {
      // Normalized categories are non-overlapping — additive pricing.
      const cachedRate =
        card.rate_cached_per_mtok ?? card.rate_input_per_mtok;
      costMinor =
        price(req.inputTokens, card.rate_input_per_mtok) +
        price(req.cachedTokens ?? 0, cachedRate) +
        price(req.outputTokens, card.rate_output_per_mtok);
      currency = card.currency;
      costBasis = "configured_rates";
      revision = card.pricing_revision;
    } else if (req.status === "unknown" && res && card) {
      // An attempted call whose usage never resolved keeps its estimate —
      // uncertain spend stays spent; a later 'reported' upgrades it.
      costMinor = res.reserved_minor;
      currency = card.currency;
      costBasis = "admission_estimate";
      revision = card.pricing_revision;
    }

    const content = {
      status: req.status,
      input_tokens: req.inputTokens ?? null,
      output_tokens: req.outputTokens ?? null,
      cached_tokens: req.cachedTokens ?? null,
      quantities: req.quantities ?? {},
      cost_minor: costMinor,
      currency,
      cost_basis: costBasis,
      pricing_revision: revision,
    };
    const sameIdentity = (f: UsageFact) =>
      f.kind === req.kind &&
      f.phase === req.phase &&
      (f.turn_id ?? "") === (req.turnId ?? "") &&
      (f.input_id ?? "") === (req.inputId ?? "") &&
      (f.round ?? 0) === (req.round ?? 0) &&
      f.funding.kind === req.funding.kind &&
      f.funding.id === req.funding.id;
    const sameFact = (f: UsageFact) =>
      sameIdentity(f) &&
      f.status === req.status &&
      (f.input_tokens ?? 0) === (req.inputTokens ?? 0) &&
      (f.output_tokens ?? 0) === (req.outputTokens ?? 0) &&
      (f.cached_tokens ?? 0) === (req.cachedTokens ?? 0);

    const existing = this.usageFacts.get(key);
    if (existing) {
      if (!sameIdentity(existing)) {
        throw new StateError(
          409,
          `fact_id ${req.factId} recorded with different content`,
        );
      }
      // Upgrade lattice (Go RecordUsage): 'reported'/'not_sent' are
      // terminal — identical replay or conflict. 'unknown'/'unrecorded'
      // can still be superseded by the call's real report.
      if (
        existing.status === "reported" ||
        existing.status === "not_sent"
      ) {
        if (!sameFact(existing)) {
          throw new StateError(
            409,
            `fact_id ${req.factId} recorded with different content`,
          );
        }
        return { fact: existing, created: false };
      }
      if (sameFact(existing)) {
        return { fact: existing, created: false };
      }
      Object.assign(existing, content);
      return { fact: existing, created: false };
    }
    const fact: UsageFact = {
      persona_id: persona,
      fact_id: req.factId,
      kind: req.kind,
      phase: req.phase,
      turn_id: req.turnId,
      input_id: req.inputId,
      round: req.round,
      funding: req.funding,
      ...content,
      recorded_at: new Date().toISOString(),
    };
    this.usageFacts.set(key, fact);
    // The hold becomes the fact's recorded cost; a 'not_sent' report
    // releases it entirely — nothing was or can be owed.
    if (res && res.status === "held") {
      res.status = req.status === "not_sent" ? "released" : "settled";
    }
    return { fact, created: true };
  }

  async listUsageFacts(persona: string, limit = 100): Promise<UsageFact[]> {
    return [...this.usageFacts.values()]
      .filter((f) => f.persona_id === persona)
      .sort((a, b) => a.recorded_at.localeCompare(b.recorded_at))
      .slice(0, limit);
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
      const input = this.inputs.find(
        (i) => i.persona_id === persona && i.input_id === running.input_id,
      );
      if (!input) throw new Error("running turn input missing");
      const rc = this.renderedContext(persona, contextLimit, input.input_id);
      return {
        turn: running,
        input,
        context: rc.events,
        memory: rc.memory,
        omitted: rc.omitted,
        memory_omitted: rc.memory_omitted,
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
    if (!input) {
      const rc = this.renderedContext(persona, contextLimit);
      return {
        turn: null,
        input: null,
        context: rc.events,
        memory: rc.memory,
        omitted: rc.omitted,
        memory_omitted: rc.memory_omitted,
        plan: null,
      };
    }
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
          (t) =>
            t.persona_id === persona &&
            t.input_id === input.input_id &&
            t.status !== "awaiting",
        ).length + 1,
      status: "running",
      started_at: new Date().toISOString(),
      finished_at: null,
      output: null,
      usage: null,
      error: null,
    };
    this.turns.set(turnId, turn);
    const rc = this.renderedContext(persona, contextLimit, input.input_id);
    return {
      turn,
      input,
      context: rc.events,
      memory: rc.memory,
      omitted: rc.omitted,
      memory_omitted: rc.memory_omitted,
      plan: this.plans.get(`${persona}|${input.input_id}`) ?? null,
    };
  }

  /**
   * The journal as the model sees it: the newest events not covered by an
   * applied chunk up to the send cap (newest always included), plus every
   * applied block admitted under the memory cap, plus the extents of older
   * raw records and applied blocks left out — same contract as Go
   * renderedContext. Records written by the loading input's own turns (a
   * note claimed before a crash, with its input_received) are left to that
   * turn, which presents its input itself.
   */
  private renderedContext(
    persona: string,
    limit: number,
    excludeInputId = "",
  ): RenderedContext {
    // Applied and superseded chunks both cover their ranges: a superseded
    // source's records are represented by the applied upper-layer block
    // that consumed it, so they no longer render raw.
    const covering = this.memoryChunks.filter(
      (c) =>
        c.persona_id === persona &&
        (c.status === "applied" || c.status === "superseded"),
    );
    const applied = covering.filter((c) => c.status === "applied");
    const uncovered = this.eventLog.filter(
      (e) =>
        e.persona_id === persona &&
        !covering.some((c) => e.seq >= c.first_seq && e.seq <= c.last_seq) &&
        !(
          excludeInputId !== "" &&
          this.turns.get(e.turn_id)?.input_id === excludeInputId
        ),
    );
    const rowCap =
      limit <= 0 ? CONTEXT_MAX_EVENTS : Math.min(limit, CONTEXT_MAX_EVENTS);
    const picked: Event[] = [];
    let budget = 0;
    for (let i = uncovered.length - 1; i >= 0; i--) {
      const e = uncovered[i];
      if (!e) break;
      const est = estEventTokens(e.kind, e.payload);
      if (
        picked.length >= rowCap ||
        (picked.length > 0 && budget + est > L0_SEND_CAP_TOKENS)
      ) {
        break;
      }
      budget += est;
      picked.push(e);
    }
    const events = picked.reverse();
    const firstShown = events[0]?.seq ?? 0;
    const older = uncovered.filter((e) => e.seq < firstShown);
    const oldest = older[0];
    const newestOmitted = older[older.length - 1];
    const omitted =
      oldest && newestOmitted
        ? {
            count: older.length,
            first_seq: oldest.seq,
            last_seq: newestOmitted.seq,
            first_time: oldest.created_at,
            last_time: newestOmitted.created_at,
          }
        : null;
    const at = (seq: number) =>
      this.eventLog.find((e) => e.persona_id === persona && e.seq === seq)
        ?.created_at ?? "";
    const blocks = applied
      .sort((a, b) => a.first_seq - b.first_seq)
      .map((c) => ({
        chunk_seq: c.chunk_seq,
        layer: c.layer,
        first_seq: c.first_seq,
        last_seq: c.last_seq,
        first_time: at(c.first_seq),
        last_time: at(c.last_seq),
        text: c.replacement ?? "",
        est_tokens: c.replacement_est_tokens ?? 0,
      }));
    const [memory, memoryOmitted] = admitApplied(blocks);
    return { events, memory, omitted, memory_omitted: memoryOmitted };
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
    // Exactly one input_received per input ever lands in the journal, and
    // every receipt names a real input (Go withoutJournaledInput +
    // received_seq): a receipt must carry a non-empty string input_id
    // naming an input row this persona holds — anything else refuses the
    // commit before any event lands. Validation covers every copy so a
    // dropped duplicate cannot mask an invalid element; a valid copy
    // naming an already-journaled input is dropped, as is a second copy
    // inside this batch — a duplicate receipt is the same fact twice.
    for (const ev of req.events) {
      if (ev.kind !== "input_received") continue;
      const id = ev.payload?.input_id;
      if (typeof id !== "string" || id === "") {
        throw new StateError(
          400,
          "input_received payload.input_id must be a non-empty string",
        );
      }
      if (
        !this.inputs.some((i) => i.persona_id === persona && i.input_id === id)
      ) {
        throw new StateError(400, `input_received names absent input ${id}`);
      }
    }
    const emitted = new Set<string>();
    for (const ev of req.events) {
      if (ev.kind === "input_received") {
        const key = `${persona}|${ev.payload.input_id as string}`;
        if (this.receivedSeq.has(key) || emitted.has(key)) {
          continue;
        }
        emitted.add(key);
      }
      const seq = this.nextSeq(this.seq, persona);
      this.eventLog.push({
        persona_id: persona,
        seq,
        turn_id: turnId,
        kind: ev.kind,
        payload: ev.payload,
        created_at: new Date().toISOString(),
      });
      if (ev.kind === "input_received") {
        this.receivedSeq.set(
          `${persona}|${ev.payload.input_id as string}`,
          seq,
        );
      }
    }
    const input = this.inputs.find(
      (i) => i.persona_id === persona && i.input_id === turn.input_id,
    );
    if (!input) throw new Error("turn input missing");
    if (req.outcome === "complete") {
      turn.status = "done";
      turn.output = req.output ?? null;
      turn.usage = req.usage ?? null;
      input.status = "done";
      input.done_at = new Date().toISOString();
      this.outboxEntries.push({
        persona_id: persona,
        seq: this.nextSeq(this.outboxSeq, persona),
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
      turn.status = "awaiting";
      if (req.wait) {
        // Mirror of the Go budget-wait commit: 'budget' is the only kind.
        // The denied admission sent no request; if the cap changed between
        // denial and commit the input requeues immediately instead of
        // waiting on a blocker that no longer exists.
        const w = req.wait;
        if (
          w.kind !== "budget" || !w.funding.kind || !w.funding.id ||
          w.needed_minor < 0 || w.currency.length !== 3
        ) {
          throw new StateError(
            400,
            "wait must be a budget wait with funding, needed_minor and currency",
          );
        }
        const budget = this.usageBudgets.get(
          `${w.funding.kind}|${w.funding.id}`,
        );
        const { spent, held } = this.fundingSpend(
          w.funding.kind,
          w.funding.id,
          budget?.currency ?? "",
        );
        const fits = !budget ||
          spent + held + w.needed_minor <= budget.limit_minor;
        if (fits) {
          input.status = "queued";
          input.claimed_generation = null;
          input.turn_id = null;
          input.not_before = null;
        } else {
          input.status = "waiting";
          input.waiting_since = new Date().toISOString();
          this.budgetWaits.set(`${persona}|${input.input_id}`, {
            persona_id: persona,
            input_id: input.input_id,
            turn_id: turnId,
            funding_kind: w.funding.kind,
            funding_id: w.funding.id,
            needed_minor: w.needed_minor,
            currency: w.currency,
            created_at: new Date().toISOString(),
          });
          this.outboxEntries.push({
            persona_id: persona,
            seq: this.nextSeq(this.outboxSeq, persona),
            kind: "budget_wait",
            payload: {
              turn_id: turnId,
              input_id: input.input_id,
              funding_kind: w.funding.kind,
              funding_id: w.funding.id,
              needed_minor: w.needed_minor,
              currency: w.currency,
            },
            created_at: new Date().toISOString(),
            delivered_at: null,
          });
        }
      } else {
      // Mirror of the Go await commit: the input waits only while an
      // approval is still pending; a decision that already landed requeues
      // it directly.
      const pending = [...this.approvals.values()].filter(
        (a) =>
          a.persona_id === persona &&
          a.input_id === input.input_id &&
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
          seq: this.nextSeq(this.outboxSeq, persona),
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
          seq: this.nextSeq(this.outboxSeq, persona),
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
    // Go reconcileHeldReservations at turn commit: this turn's held
    // reservations settle when their fact landed; a call whose record
    // never arrived keeps its estimate as an 'unrecorded' fact — a lost
    // response does not prove the request never left.
    for (const [k, r] of this.usageReservations) {
      if (
        k.startsWith(`${persona}|`) &&
        r.turn_id === turnId &&
        r.status === "held"
      ) {
        this.reconcileHeldReservation(k, r);
      }
    }
    return turn;
  }

  /**
   * Reconcile one held reservation (Go reconcileHeldReservations): if the
   * fact exists the hold just settles; otherwise an 'unrecorded' fact
   * carries the admission estimate as 'admission_estimate' spend so
   * possibly-sent money cannot silently restore the allowance.
   */
  private reconcileHeldReservation(
    key: string,
    r: (typeof this.usageReservations extends Map<string, infer V> ? V : never),
  ) {
    if (!this.usageFacts.has(key)) {
      const persona = key.slice(0, key.indexOf("|"));
      this.usageFacts.set(key, {
        persona_id: persona,
        fact_id: r.fact_id,
        kind: r.kind,
        phase: r.phase,
        turn_id: r.turn_id ?? undefined,
        input_id: r.input_id ?? undefined,
        round: r.round,
        funding: r.funding,
        status: "unrecorded",
        input_tokens: null,
        output_tokens: null,
        cached_tokens: null,
        quantities: {},
        cost_minor: r.currency !== null ? r.reserved_minor : null,
        currency: r.currency ?? undefined,
        cost_basis: r.currency !== null ? "admission_estimate" : undefined,
        pricing_revision: r.pricing_revision ?? undefined,
        recorded_at: new Date().toISOString(),
      });
    }
    r.status = "settled";
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
  ): Promise<{
    operation: Operation;
    approval: Approval | null;
    fresh: boolean;
  }> {
    // Unregistered tools are rejected at the boundary (Go ErrUnknownTool →
    // 400), before the fence check — a dangling 'running' op is never
    // recorded for a tool no executor can finish. Go's claimableTool is
    // internal tools ∪ registered effects; the fake mirrors both.
    if (!(op.tool in TOOL_AUTHORITY) && !this.registeredEffects.has(op.tool)) {
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
        return this.claimGated(
          persona,
          turn.input_id,
          op.callIndex,
          existing,
          false,
        );
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
      this.applyInternal(
        persona,
        turn.input_id,
        op.callIndex,
        op.turnId,
        operation,
      );
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
      throw new StateError(
        500,
        `approval record missing for ${op.operation_id}`,
      );
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
      const seq = this.nextSeq(this.outboxSeq, persona);
      this.outboxEntries.push({
        persona_id: persona,
        seq,
        kind: "secretary_message",
        payload: { text, turn_id: turnId },
        created_at: new Date().toISOString(),
        delivered_at: null,
      });
      operation.response = {
        seq,
        kind: "secretary_message",
      };
    } else if (operation.tool === "journal.note") {
      const text = op.text;
      if (typeof text !== "string" || text === "") {
        throw new StateError(400, "journal.note requires text");
      }
      // The note is part of this input's experience: the input is journaled
      // first, once, so the note never precedes what it responds to.
      const turn = this.turns.get(turnId);
      if (turn) this.ensureInputReceived(persona, turn);
      const ev: Event = {
        persona_id: persona,
        seq: this.nextSeq(this.seq, persona),
        turn_id: turnId,
        kind: "note",
        payload: { text },
        created_at: new Date().toISOString(),
      };
      this.eventLog.push(ev);
      operation.response = { seq: ev.seq, kind: "note" };
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
    } else if (operation.tool === "conversation_history") {
      // Read-only; runs inside the claim like Go so the receipt reflects
      // the same committed view.
      operation.response = this.historyTool(persona, op);
    } else {
      const effect = this.registeredEffects.get(operation.tool);
      if (effect !== undefined) {
        // Delegated effect — Go runs Apply inside the claim transaction.
        const response = effect(persona, `${inputId}:tool:${callIndex}`, op);
        operation.response = response as Record<string, unknown>;
      } else {
        // A registered tool with no effect case fails the claim outright —
        // Go rolls the insert back (no phantom row), so a later claim
        // reports the same deterministic error (review f44).
        throw new StateError(
          400,
          `${operation.tool} has no registered effect to run`,
        );
      }
    }
    operation.status = "done";
    operation.completed_at = new Date().toISOString();
  }

  async listApprovals(
    persona: string,
    approvalId?: string,
  ): Promise<Approval[]> {
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
    if (
      decision.decision !== "approve_once" &&
      decision.decision !== "deny_once"
    ) {
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
      seq: this.nextSeq(this.seq, persona),
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
    // An explicit test binding overrides, as before. Otherwise a carried
    // model_intent mirrors the Go gate: the persona may not fall back to
    // 'unset' — it reports needs_rebinding until the test binds a human
    // and provides a matching selection (setModelBinding), or clears the
    // intent (setModelIntent null).
    const explicit = this.modelBindings.get(persona);
    if (explicit) return Promise.resolve(explicit);
    const intent = this.personas.get(persona)?.model_intent ?? null;
    if (intent) {
      return Promise.resolve({
        selection: "needs_rebinding",
        intent: intent as ModelBinding["intent"],
        reason: `carried model intent '${intent.kind}' needs a destination selection`,
      });
    }
    return Promise.resolve({ selection: "unset" });
  }

  /**
   * conversation_history: opens this persona's stored journal records —
   * recorded history, not implicit recollection. Mirrors the Go backend:
   * literal case-sensitive substring search over each record's stored text
   * (or serialized payload); read pages by a 16,384-character budget over
   * the stable journal_event_v1 serialization, with content_offset
   * fragments for oversized records.
   */
  private historyTool(persona: string, request: Json): Json {
    const num = (k: string): number | null => {
      const v = request[k];
      return v === undefined ? null : (v as number);
    };
    const operation = request.operation as string | undefined;
    const query = request.query as string | undefined;
    const seq = num("seq");
    const chunkSeq = num("chunk_seq");
    const fromSeq = num("from_seq");
    const afterSeq = num("after_seq");
    const contentOffset = num("content_offset");
    const limit = (request.limit as number | undefined) ?? 5;
    const bad = (m: string) => new StateError(400, `bad request: ${m}`);
    if (operation !== "search" && operation !== "read") {
      throw bad("operation must be search or read");
    }
    if ((operation === "search") !== (query !== undefined)) {
      throw bad("search requires query and read must not carry one");
    }
    if (query === "") throw bad("search query must not be empty");
    if (limit < 1 || limit > 20 || !Number.isInteger(limit)) {
      throw bad("limit must be an integer in [1, 20]");
    }
    if (contentOffset !== null && (operation !== "read" || seq === null)) {
      throw bad("content_offset is only valid with read + seq");
    }
    const locators = [seq, chunkSeq, fromSeq].filter((v) => v !== null).length;
    if (locators > 1)
      throw bad("seq, chunk_seq and from_seq are alternative locators");
    // chunk_seq + after_seq continues a page within that chunk's range.
    if (seq !== null && afterSeq !== null) {
      throw bad("seq cannot combine with after_seq");
    }
    const all = this.eventLog.filter((e) => e.persona_id === persona);
    const details = (
      messages: Json[],
      nextAfterSeq: number | null,
      nextRead: Json | null,
    ): Json => ({
      operation,
      scope: "your_conversation_history",
      messages,
      next_after_seq: nextAfterSeq,
      next_read: nextRead,
      has_more: nextAfterSeq !== null || nextRead !== null,
      journal_event_format: "journal_event_v1",
      content_complete_meaning:
        "entire stored event representation returned in this call; does not imply provider vision support",
      fragment_continuation:
        "follow next_read to the end of this event before resuming the original query with after_seq=resume_after_seq; concatenated fragments form the journal_event_v1 JSON",
      search_coverage:
        "literal case-sensitive substring of each record's stored text field or of its journal_event_v1 JSON exactly as read returns it (sorted keys, no added spaces); at most 2000 records are scanned per call, continue with next_after_seq; no match is not proof that a record is absent",
    });
    const source = (e: Event): Json => ({
      seq: e.seq,
      turn_id: e.turn_id,
      kind: e.kind,
    });
    const journalJson = journalEventJson;
    if (operation === "search") {
      const q = query as string;
      const hits: { e: Event; text: string; source: string }[] = [];
      let scanned = 0;
      let budgetReached = false;
      let cursor = afterSeq ?? 0;
      for (const e of all) {
        if (e.seq <= (afterSeq ?? 0)) continue;
        if (scanned >= HISTORY_SEARCH_SCAN_RECORDS) {
          budgetReached = true;
          break;
        }
        scanned++;
        cursor = e.seq;
        const t = e.payload.text;
        if (typeof t === "string" && t.includes(q)) {
          hits.push({ e, text: t, source: "text" });
        } else {
          const serialized = journalJson(e);
          if (serialized.includes(q)) {
            hits.push({ e, text: serialized, source: "journal_event_v1" });
          }
        }
        if (hits.length > limit) break;
      }
      let nextAfterSeq: number | null = null;
      if (hits.length > limit) {
        hits.length = limit;
        nextAfterSeq = (hits[limit - 1] as { e: Event }).e.seq;
      } else if (budgetReached) {
        nextAfterSeq = cursor;
      }
      const messages = hits.map((h) => {
        const runes = [...h.text];
        const byteIndex = h.text.indexOf(q);
        const matchStart =
          byteIndex < 0 ? 0 : [...h.text.slice(0, byteIndex)].length;
        const start = Math.max(0, matchStart - 80);
        const end = Math.min(start + 500, runes.length);
        return {
          source: source(h.e),
          timestamp: h.e.created_at,
          snippet: runes.slice(start, end).join(""),
          snippet_source: h.source,
          snippet_char_start: start,
          snippet_truncated: start > 0 || end < runes.length,
        };
      });
      return {
        ...details(messages, nextAfterSeq, null),
        scanned_records: scanned,
        scan_budget_reached: budgetReached,
      };
    }
    // read
    let events: Event[];
    let upper: number | null = null;
    if (seq !== null) {
      const e = all.find((ev) => ev.seq === seq);
      if (!e) throw bad("journal event not found");
      events = [e];
    } else {
      let from = 1;
      if (chunkSeq !== null) {
        const c = this.memoryChunks.find(
          (x) => x.persona_id === persona && x.chunk_seq === chunkSeq,
        );
        // A wrong locator is the model's bad request, recorded as the tool
        // result — not a state outage (Go maps it to 400 too).
        if (!c) throw bad(`memory chunk ${chunkSeq} not found`);
        from = c.first_seq;
        upper = c.last_seq;
      }
      if (fromSeq !== null) from = fromSeq;
      if (afterSeq !== null && afterSeq + 1 > from) from = afterSeq + 1;
      events = all
        .filter((e) => e.seq >= from && (upper === null || e.seq <= upper))
        .slice(0, limit + 1);
    }
    const messages: Json[] = [];
    let nextAfterSeq: number | null = null;
    let nextRead: Json | null = null;
    let readChars = 0;
    let lastCompleteSeq: number | null = null;
    for (let i = 0; i < events.length; i++) {
      const e = events[i];
      if (!e) break;
      const serialized = journalJson(e);
      const runes = [...serialized];
      const totalChars = runes.length;
      const offset = contentOffset ?? 0;
      if (offset > totalChars) {
        throw bad(
          `content_offset ${offset} beyond record length ${totalChars}`,
        );
      }
      if (
        i >= limit ||
        (offset === 0 &&
          messages.length > 0 &&
          readChars + totalChars > HISTORY_READ_CHAR_BUDGET)
      ) {
        if (lastCompleteSeq !== null) nextAfterSeq = lastCompleteSeq;
        break;
      }
      if (offset === 0 && readChars + totalChars <= HISTORY_READ_CHAR_BUDGET) {
        readChars += totalChars;
        lastCompleteSeq = e.seq;
        messages.push({
          source: source(e),
          event: JSON.parse(serialized),
          content_complete: true,
        });
        continue;
      }
      const end = Math.min(offset + HISTORY_READ_CHAR_BUDGET, totalChars);
      if (end < totalChars) {
        nextRead = { operation: "read", seq: e.seq, content_offset: end };
      }
      let more = i + 1 < events.length;
      if (!more && seq === null) {
        more = all.some(
          (ev) => ev.seq > e.seq && (upper === null || ev.seq <= upper),
        );
      }
      if (more) nextAfterSeq = e.seq;
      messages.push({
        source: source(e),
        timestamp: e.created_at,
        event_json_fragment: runes.slice(offset, end).join(""),
        event_json_range: {
          start_char: offset,
          end_char: end,
          total_chars: totalChars,
          ends_event: end === totalChars,
        },
        content_complete: false,
        resume_after_seq: e.seq,
      });
      break;
    }
    return details(messages, nextAfterSeq, nextRead);
  }

  /**
   * Memory housekeeping between turns: seal newly safe journal ranges, then
   * apply shelved replacements while the live raw estimate exceeds the
   * limit — same contract as Go MemoryMaintain.
   */
  async memoryMaintain(
    persona: string,
    generation: number,
  ): Promise<MemoryStatus> {
    this.mustHold(persona, generation);
    this.interruptPreparing(persona, generation);
    const mine = () =>
      this.memoryChunks.filter((c) => c.persona_id === persona);
    const covered = Math.max(0, ...mine().map((c) => c.last_seq));
    // Seal walk: accumulate the unsealed tail; cut a chunk just before each
    // input_received once the window reaches the minimum and no tool call
    // in it is still waiting for its result. Past the forced limit, one
    // further boundary kind opens — before an assistant_message that does
    // not directly continue a tool flow. A turn's deciding text and the
    // calls/results it started are one unit: never cut before a tool_call
    // or tool_result, and a flow with no interior boundary seals whole past
    // the limit. Same rules as the Go walk.
    const tail = this.eventLog.filter(
      (e) => e.persona_id === persona && e.seq > covered,
    );
    const pending = new Set<string>();
    let windowEst = 0;
    let windowStart = -1;
    let window: Event[] = [];
    let prevKind = "";
    for (const e of tail) {
      let cut = false;
      if (windowStart >= 0 && pending.size === 0) {
        switch (e.kind) {
          case "input_received":
            cut = windowEst >= L0_CHUNK_MIN_TOKENS;
            break;
          case "assistant_message":
            cut =
              windowEst > L0_FORCED_SEAL_LIMIT_TOKENS &&
              prevKind !== "tool_result";
            break;
        }
      }
      if (cut) {
        const nextChunkSeq = this.nextChunkSeq(persona);
        this.memoryChunks.push({
          persona_id: persona,
          chunk_seq: nextChunkSeq,
          layer: 1,
          sources: null,
          first_seq: windowStart,
          last_seq: window[window.length - 1]?.seq ?? windowStart,
          est_tokens: windowEst,
          status: "sealed",
          replacement: null,
          replacement_est_tokens: null,
          attempts: 0,
          interruptions: 0,
          last_error: null,
          claimed_generation: null,
          claimed_at: null,
          not_before: null,
          created_at: new Date().toISOString(),
          prepared_at: null,
          applied_at: null,
        });
        window = [];
        windowEst = 0;
        windowStart = -1;
      }
      if (windowStart < 0) windowStart = e.seq;
      window.push(e);
      windowEst += estEventTokens(e.kind, e.payload);
      const callId = e.payload.call_id;
      if (e.kind === "tool_call" && typeof callId === "string" && callId) {
        pending.add(callId);
      } else if (e.kind === "tool_result" && typeof callId === "string") {
        pending.delete(callId);
      }
      prevKind = e.kind;
    }
    // Live raw = every not-yet-applied layer-1 chunk plus the unsealed
    // tail. Upper-layer rows and superseded sources never count: their
    // ranges are represented by applied replacements, not raw events.
    let live =
      windowEst +
      mine()
        .filter(
          (c) =>
            c.layer === 1 &&
            c.status !== "applied" &&
            c.status !== "superseded",
        )
        .reduce((s, c) => s + c.est_tokens, 0);
    if (live > L0_LIVE_LIMIT_TOKENS) {
      for (const c of mine()
        .filter((c) => c.layer === 1 && c.status === "prepared")
        .sort((a, b) => a.chunk_seq - b.chunk_seq)) {
        if (live <= L0_LIVE_LIMIT_TOKENS) break;
        c.status = "applied";
        c.applied_at = new Date().toISOString();
        live -= c.est_tokens;
      }
    }
    // A prepared upper-layer target applies once the layer it consumes is
    // still over its own limit; its sources become 'superseded' in the same
    // step so their ranges are represented by the target, not dropped.
    const appliedTokens = (layer: number) =>
      mine()
        .filter((c) => c.layer === layer && c.status === "applied")
        .reduce((s, c) => s + (c.replacement_est_tokens ?? 0), 0);
    for (const target of mine()
      .filter((c) => c.layer >= 2 && c.status === "prepared")
      .sort((a, b) => a.chunk_seq - b.chunk_seq)) {
      const srcs = (target.sources ?? []).map((seq) =>
        mine().find((s) => s.chunk_seq === seq),
      );
      const srcLayer = srcs[0]?.layer ?? 0;
      const stale =
        srcs.length !== (target.sources ?? []).length ||
        srcs.some((s) => s?.status !== "applied" || s.layer !== srcLayer) ||
        (srcLayer !== 1 && srcLayer !== 2);
      if (stale) {
        target.status = "failed";
        target.last_error =
          "upper-layer target is stale: its selected sources are no longer applied";
        continue;
      }
      const limit = srcLayer === 1 ? L1_LIMIT_TOKENS : L2_LIMIT_TOKENS;
      if (appliedTokens(srcLayer) <= limit) continue;
      for (const s of srcs) {
        if (s) s.status = "superseded";
      }
      target.status = "applied";
      target.applied_at = new Date().toISOString();
    }
    // Create the next upper-layer target: one in flight at a time. L1→L2
    // consumes the oldest contiguous applied L1 run until the remainder
    // drops to L1_DROP_TO; L2 reintegration takes the whole contiguous
    // applied L2 run. A 'kept' target's exact source tuple is never
    // re-selected.
    const busy = mine().some(
      (c) =>
        c.layer >= 2 &&
        (c.status === "sealed" ||
          c.status === "preparing" ||
          c.status === "prepared"),
    );
    if (!busy) {
      for (const sel of [
        {
          srcLayer: 1,
          limit: L1_LIMIT_TOKENS,
          dropTo: L1_DROP_TO_TOKENS,
          whole: false,
        },
        {
          srcLayer: 2,
          limit: L2_LIMIT_TOKENS,
          dropTo: L2_LIMIT_TOKENS,
          whole: true,
        },
      ]) {
        const total = appliedTokens(sel.srcLayer);
        if (total <= sel.limit) continue;
        const frags = mine()
          .filter((c) => c.layer === sel.srcLayer && c.status === "applied")
          .sort((a, b) => a.first_seq - b.first_seq);
        for (let i = 0; i < frags.length; ) {
          const group = [frags[i] as MemoryChunk];
          let consumed = group[0]?.replacement_est_tokens ?? 0;
          i++;
          while (
            i < frags.length &&
            (frags[i] as MemoryChunk).first_seq ===
              (group[group.length - 1] as MemoryChunk).last_seq + 1 &&
            (sel.whole || consumed < total - sel.dropTo)
          ) {
            const f = frags[i] as MemoryChunk;
            group.push(f);
            consumed += f.replacement_est_tokens ?? 0;
            i++;
          }
          const srcSeqs = group.map((f) => f.chunk_seq);
          // A 'kept' or 'failed' verdict settles its exact source tuple —
          // resealing it would relitigate the answer or burn a fresh
          // attempt budget forever. A different grouping may still run.
          const dup = mine().some(
            (c) =>
              c.layer >= 2 &&
              (c.status === "kept" || c.status === "failed") &&
              c.sources !== null &&
              c.sources.length === srcSeqs.length &&
              c.sources.every((s, j) => s === srcSeqs[j]),
          );
          if (dup) continue;
          const nextChunkSeq = this.nextChunkSeq(persona);
          this.memoryChunks.push({
            persona_id: persona,
            chunk_seq: nextChunkSeq,
            layer: 2,
            sources: srcSeqs,
            first_seq: (group[0] as MemoryChunk).first_seq,
            last_seq: (group[group.length - 1] as MemoryChunk).last_seq,
            est_tokens: consumed,
            status: "sealed",
            replacement: null,
            replacement_est_tokens: null,
            attempts: 0,
            interruptions: 0,
            last_error: null,
            claimed_generation: null,
            claimed_at: null,
            not_before: null,
            created_at: new Date().toISOString(),
            prepared_at: null,
            applied_at: null,
          });
          return this.memoryStatus(persona);
        }
      }
    }
    return this.memoryStatus(persona);
  }

  async memoryStatus(persona: string): Promise<MemoryStatus> {
    const mine = this.memoryChunks.filter((c) => c.persona_id === persona);
    const covered = Math.max(0, ...mine.map((c) => c.last_seq));
    const events = this.eventLog.filter((e) => e.persona_id === persona);
    const tail = events
      .filter((e) => e.seq > covered)
      .reduce((s, e) => s + estEventTokens(e.kind, e.payload), 0);
    const count = (s: MemoryChunk["status"]) =>
      mine.filter((c) => c.status === s).length;
    const now = Date.now();
    const readyAt = (c: MemoryChunk): number | null =>
      c.status === "preparing"
        ? now
        : c.status === "sealed"
          ? Math.max(c.not_before ? Date.parse(c.not_before) : now, now)
          : null;
    const ready = mine.map(readyAt).filter((t): t is number => t !== null);
    const appliedBlocks = mine
      .filter((c) => c.status === "applied")
      .sort((a, b) => a.first_seq - b.first_seq)
      .map(
        (c) => ({ est_tokens: c.replacement_est_tokens ?? 0 }) as MemoryBlock,
      );
    return {
      live_raw_tokens:
        mine
          .filter(
            (c) =>
              c.layer === 1 &&
              c.status !== "applied" &&
              c.status !== "superseded",
          )
          .reduce((s, c) => s + c.est_tokens, 0) + tail,
      applied_tokens: mine
        .filter((c) => c.status === "applied")
        .reduce((s, c) => s + (c.replacement_est_tokens ?? 0), 0),
      sealed: count("sealed"),
      preparing: count("preparing"),
      prepared: count("prepared"),
      applied: count("applied"),
      kept: count("kept"),
      failed: count("failed"),
      superseded: count("superseded"),
      claimable: ready.filter((t) => t <= now).length,
      next_claimable_at: ready.length
        ? new Date(Math.min(...ready)).toISOString()
        : null,
      applied_omitted: admitApplied(appliedBlocks)[1]?.count ?? 0,
      covered_seq: covered,
      latest_seq: Math.max(0, ...events.map((e) => e.seq)),
      chunk_min_tokens: L0_CHUNK_MIN_TOKENS,
      live_limit_tokens: L0_LIVE_LIMIT_TOKENS,
      memory_send_cap_tokens: MEMORY_SEND_CAP_TOKENS,
    };
  }

  async claimMemoryChunk(
    persona: string,
    generation: number,
    contextLimit: number,
  ): Promise<ClaimedMemoryChunk> {
    this.mustHold(persona, generation);
    const mine = this.memoryChunks.filter((c) => c.persona_id === persona);
    const empty = () => ({
      chunk: null,
      target_events: [],
      target_fragments: [],
      context: this.renderedContext(persona, contextLimit),
    });
    // Every 'preparing' chunk is an orphan from the caller's view: its claim
    // ended without an outcome, so it counts an interruption — not an
    // attempt — and waits out a short pacing. (FakeState is single-threaded;
    // the real store relies on pacing and generation fencing to converge
    // concurrent claims, not on strict single-flight.)
    this.interruptPreparing(persona, null);
    const c = mine
      .filter(
        (x) =>
          x.status === "sealed" &&
          (x.not_before === null || Date.parse(x.not_before) <= Date.now()),
      )
      .sort((a, b) => a.chunk_seq - b.chunk_seq)[0];
    if (!c) return empty();
    c.status = "preparing";
    c.claimed_generation = generation;
    c.claimed_at = new Date().toISOString();
    c.not_before = null;
    if (c.layer >= 2) {
      // An upper-layer target prepares from its selected sources' accepted
      // texts, not raw events. A stale target (a source no longer applied)
      // is marked failed without spending attempts — the honest answer for
      // a carried row or one whose sources another target consumed.
      const at = (seq: number) =>
        this.eventLog.find((e) => e.persona_id === persona && e.seq === seq)
          ?.created_at ?? "";
      const srcs = (c.sources ?? []).map((seq) =>
        this.memoryChunks.find(
          (s) => s.persona_id === persona && s.chunk_seq === seq,
        ),
      );
      const stale =
        srcs.length !== (c.sources ?? []).length ||
        srcs.some((s) => s?.status !== "applied");
      if (stale) {
        c.status = "failed";
        c.claimed_generation = null;
        c.claimed_at = null;
        c.last_error =
          "upper-layer target is stale: its selected sources are no longer applied";
        return empty();
      }
      const fragments = (srcs as MemoryChunk[]).map((s) => ({
        chunk_seq: s.chunk_seq,
        layer: s.layer,
        first_seq: s.first_seq,
        last_seq: s.last_seq,
        first_time: at(s.first_seq),
        last_time: at(s.last_seq),
        text: s.replacement ?? "",
        est_tokens: s.replacement_est_tokens ?? 0,
      }));
      return {
        chunk: c,
        target_events: [],
        target_fragments: fragments,
        context: this.renderedContext(persona, contextLimit),
      };
    }
    return {
      chunk: c,
      target_events: this.eventLog.filter(
        (e) =>
          e.persona_id === persona &&
          e.seq >= c.first_seq &&
          e.seq <= c.last_seq,
      ),
      target_fragments: [],
      context: this.renderedContext(persona, contextLimit),
    };
  }

  async completeMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    result: { replacement?: string; keepUnchanged?: boolean },
  ): Promise<MemoryChunk> {
    this.mustHold(persona, generation);
    const replacement = result.replacement ?? "";
    const keepUnchanged = result.keepUnchanged ?? false;
    if (keepUnchanged === (replacement !== "")) {
      throw new StateError(
        400,
        "bad request: exactly one of replacement text or keep_unchanged is required",
      );
    }
    if (replacement.includes("\u0000")) {
      throw new StateError(400, "bad request: replacement contains a NUL byte");
    }
    const c = this.memoryChunks.find(
      (x) => x.persona_id === persona && x.chunk_seq === chunkSeq,
    );
    if (!c) throw new StateError(404, "memory chunk not found");
    if (c.status === "preparing") {
      if (c.claimed_generation !== generation) throw new FencedError();
      const rest = estTextTokens(replacement);
      if (keepUnchanged) {
        c.status = "kept";
      } else if (rest >= c.est_tokens) {
        // A replacement that does not shrink the range is kept visible but
        // never applied: the originals stay in context.
        c.status = "kept";
        c.replacement = replacement;
        c.replacement_est_tokens = rest;
        c.last_error = `replacement did not shrink the range (${rest} >= ${c.est_tokens} estimated tokens); originals kept`;
      } else {
        c.status = "prepared";
        c.replacement = replacement;
        c.replacement_est_tokens = rest;
      }
      c.claimed_generation = null;
      c.claimed_at = null;
      c.prepared_at = new Date().toISOString();
      return c;
    }
    if (
      c.status === "prepared" ||
      c.status === "kept" ||
      c.status === "applied"
    ) {
      const same =
        (keepUnchanged && c.status === "kept" && c.replacement === null) ||
        (!keepUnchanged && c.replacement === replacement);
      if (!same) {
        throw new StateError(
          409,
          `chunk ${chunkSeq} already completed with different content`,
        );
      }
      return c;
    }
    throw new StateError(
      409,
      `chunk ${chunkSeq} is ${c.status}, not preparing`,
    );
  }

  async failMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    failure: { error: string; retryable: boolean },
  ): Promise<MemoryChunk> {
    this.mustHold(persona, generation);
    const c = this.memoryChunks.find(
      (x) => x.persona_id === persona && x.chunk_seq === chunkSeq,
    );
    if (!c) throw new StateError(404, "memory chunk not found");
    if (c.status !== "preparing" || c.claimed_generation !== generation) {
      throw new StateError(
        409,
        `chunk ${chunkSeq} is not preparing under this generation`,
      );
    }
    // A recorded failure is the only thing that spends attempts.
    c.attempts += 1;
    c.claimed_generation = null;
    c.claimed_at = null;
    c.last_error = failure.error;
    if (failure.retryable && c.attempts < MEMORY_CHUNK_MAX_ATTEMPTS) {
      c.status = "sealed";
      c.not_before = new Date(
        Date.now() + retryBackoffMs(c.attempts),
      ).toISOString();
    } else {
      c.status = "failed";
      c.not_before = null;
    }
    return c;
  }

  /**
   * Go ReshelveMemoryChunk: no model request could be evaluated — the
   * binding was unavailable or budget admission denied the call — so the
   * claim records no verdict and spends neither attempts nor
   * interruptions; a short pacing keeps a persistent condition from
   * claiming every tick, and a 'budget-wait:' reason is cleared early by
   * a funding change.
   */
  async reshelveMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    pause: { reason: string; delayMs?: number },
  ): Promise<MemoryChunk> {
    this.mustHold(persona, generation);
    const c = this.memoryChunks.find(
      (x) => x.persona_id === persona && x.chunk_seq === chunkSeq,
    );
    if (!c) throw new StateError(404, "memory chunk not found");
    if (c.status !== "preparing" || c.claimed_generation !== generation) {
      throw new StateError(
        409,
        `chunk ${chunkSeq} is not preparing under this generation`,
      );
    }
    c.status = "sealed";
    c.claimed_generation = null;
    c.claimed_at = null;
    c.last_error = pause.reason;
    c.not_before = new Date(
      Date.now() + (pause.delayMs ?? MEMORY_RESHELVE_PACING_MS),
    ).toISOString();
    return c;
  }

  /**
   * Go interruptPreparing: a 'preparing' claim that ended without an
   * outcome (host stopped, fence lost, lost response) counts one
   * interruption and returns to the shelf after a short pacing; too many
   * mark the chunk failed, visible with its originals kept.
   */
  private interruptPreparing(persona: string, exceptGeneration: number | null) {
    for (const c of this.memoryChunks) {
      if (
        c.persona_id !== persona ||
        c.status !== "preparing" ||
        (exceptGeneration !== null && c.claimed_generation === exceptGeneration)
      ) {
        continue;
      }
      const prior = c.interruptions;
      c.interruptions += 1;
      c.claimed_generation = null;
      c.claimed_at = null;
      if (c.interruptions >= MEMORY_CHUNK_MAX_INTERRUPTIONS) {
        c.status = "failed";
        c.not_before = null;
        c.last_error = [
          c.last_error,
          `preparation was interrupted ${MEMORY_CHUNK_MAX_INTERRUPTIONS} times without a recorded outcome`,
        ]
          .filter(Boolean)
          .join("; ");
      } else {
        c.status = "sealed";
        c.not_before = new Date(
          Date.now() + Math.min(200 * 2 ** Math.min(prior, 8), 30_000),
        ).toISOString();
      }
    }
  }

  /** Go ensureInputReceived: journal the turn's input once, before a note. */
  private ensureInputReceived(persona: string, turn: Turn) {
    const key = `${persona}|${turn.input_id}`;
    if (this.receivedSeq.has(key)) return;
    const input = this.inputs.find(
      (i) => i.persona_id === persona && i.input_id === turn.input_id,
    );
    if (!input) throw new Error("turn input missing");
    const seq = this.nextSeq(this.seq, persona);
    this.eventLog.push({
      persona_id: persona,
      seq,
      turn_id: turn.turn_id,
      kind: "input_received",
      payload: {
        input_id: input.input_id,
        kind: input.kind,
        text:
          typeof input.payload.text === "string" ? input.payload.text : null,
        actor_kind: input.actor_kind,
        source_surface: input.source_surface,
        attempt: turn.attempt,
      },
      created_at: new Date().toISOString(),
    });
    this.receivedSeq.set(key, seq);
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

  /**
   * Test wiring — Go Store.RegisterEffect parity. The tool becomes
   * claimable and advertised by listTools; its applier runs inside the
   * claim like Go's in-transaction Apply.
   */
  registerEffect(
    tool: string,
    apply: (
      persona: string,
      idemKey: string,
      request: Record<string, unknown>,
    ) => Json,
  ): void {
    if (tool in TOOL_AUTHORITY) {
      throw new StateError(400, `${tool} is a built-in internal tool`);
    }
    this.registeredEffects.set(tool, apply);
  }

  async listTools(_persona: string): Promise<string[]> {
    return [
      ...Object.keys(TOOL_AUTHORITY),
      ...this.registeredEffects.keys(),
    ].sort();
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
