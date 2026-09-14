import { jsonEqual } from "./json.ts";
import { FencedError, type StateClient, StateError } from "./state-client.ts";
import type {
  ClaimedMemoryChunk,
  CommitRequest,
  Event,
  Input,
  Job,
  JobTerminalReport,
  Json,
  LoadResult,
  MemoryBlock,
  MemoryChunk,
  MemoryStatus,
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
  /** Sealed journal ranges and their L1 replacement lifecycle. */
  memoryChunks: MemoryChunk[] = [];
  /** First commit request per turn — replay comparison (commit_request). */
  private commits = new Map<string, CommitRequest>();
  /** Seq of each input's one input_received event (core_inputs.received_seq). */
  private receivedSeq = new Map<string, number>();
  /** Per-persona seqs — Go allocates MAX(seq)+1 per persona for both
   *  core_events and core_outbox, so a second persona starts at 1. */
  private seq = new Map<string, number>();
  private outboxSeq = new Map<string, number>();

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

  addPersona(personaId: string, displayName = personaId) {
    this.personas.set(personaId, {
      human_id: null,
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
    // Memory chunks a fenced generation was preparing count an interruption.
    this.interruptPreparing(persona, generation);
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
      attempt:
        [...this.turns.values()].filter(
          (t) => t.persona_id === persona && t.input_id === input.input_id,
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
  ): Promise<{ operation: Operation; fresh: boolean }> {
    // Unregistered tools are rejected at the boundary (Go ErrUnknownTool →
    // 400), before the fence check — a dangling 'running' op is never
    // recorded for a tool no executor can finish.
    if (
      op.tool !== "schedule.set" &&
      op.tool !== "journal.note" &&
      op.tool !== "conversation_history" &&
      op.tool !== "job.start" &&
      op.tool !== "job.status" &&
      op.tool !== "job.cancel"
    ) {
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
        return { operation: existing, fresh: true };
      }
      // A replayed job.* receipt carries the job's state now next to the
      // original result (Go withCurrentJobTx); the stored receipt stays.
      if (op.tool.startsWith("job.") && existing.status === "done") {
        return {
          operation: {
            ...existing,
            response: this.withCurrentJob(persona, existing.response),
          },
          fresh: false,
        };
      }
      return { operation: existing, fresh: false };
    }
    const operation: Operation = {
      persona_id: persona,
      operation_id: op.operationId,
      turn_id: op.turnId,
      tool: op.tool,
      idempotency_key: idempotencyKey,
      request: op.request,
      status: "done",
      response: null,
      claimed_generation: generation,
      created_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
    };
    if (op.tool === "schedule.set") {
      const missPolicy = (op.request.miss_policy as string) ?? "fire_late";
      if (!MISS_POLICIES.has(missPolicy)) {
        throw new StateError(
          400,
          "schedule.set miss_policy must be fire_late, coalesce, expire, or report_missed",
        );
      }
      const wakeAt = new Date(String(op.request.wake_at));
      if (Number.isNaN(wakeAt.getTime())) {
        throw new StateError(400, "schedule.set requires RFC3339 wake_at");
      }
      const sid = (op.request.schedule_id as string) ?? `sch-${Date.now()}`;
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
            (op.request.payload as Record<string, unknown>) ?? {},
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
          payload: (op.request.payload as Record<string, unknown>) ?? {},
          miss_policy: missPolicy as Schedule["miss_policy"],
          status: "pending",
          claimed_generation: null,
          created_at: new Date().toISOString(),
          fired_at: null,
        };
        this.schedules.set(`${persona}|${sid}`, sch);
        operation.response = { schedule: sch };
      }
    } else if (op.tool === "journal.note") {
      // The note is part of this input's experience: the input is journaled
      // first, once, so the note never precedes what it responds to.
      this.ensureInputReceived(persona, turn);
      const ev: Event = {
        persona_id: persona,
        seq: this.nextSeq(this.seq, persona),
        turn_id: op.turnId,
        kind: "note",
        payload: { text: op.request.text },
        created_at: new Date().toISOString(),
      };
      this.eventLog.push(ev);
      operation.response = { seq: ev.seq, kind: "note" };
    } else if (op.tool === "job.start") {
      // The job row is minted inside this claim transaction with a
      // server-derived id (plan position), so a replayed claim can never
      // mint a second job and the model never chooses an id.
      validateSubprocessRequest(op.request);
      const jobId = `op:${turn.input_id}:${op.callIndex}`;
      const job = this.insertJob(
        persona,
        jobId,
        "subprocess",
        op.request,
        `tool:${op.turnId}:${op.callIndex}`,
      );
      // Receipts are snapshots (Go stores jsonb), never the live job row.
      operation.response = { job: structuredClone(job) };
    } else if (op.tool === "job.status") {
      const jobId = op.request.job_id;
      if (typeof jobId !== "string" || jobId === "") {
        throw new StateError(400, "job.status requires job_id");
      }
      // Internal-tool boundary: Go maps ErrJobNotFound to 400 here so a
      // missing job is a recorded tool error, not a transient retry.
      operation.response = {
        job: structuredClone(this.mustJob400(persona, jobId)),
      };
    } else if (op.tool === "job.cancel") {
      const jobId = op.request.job_id;
      if (typeof jobId !== "string" || jobId === "") {
        throw new StateError(400, "job.cancel requires job_id");
      }
      operation.response = {
        job: structuredClone(this.cancelJobRow(persona, jobId, true)),
      };
    } else {
      // conversation_history: read-only; runs inside the claim like Go so
      // the receipt reflects the same committed view.
      operation.response = this.historyTool(persona, op.request);
    }
    this.ops.set(k, operation);
    return { operation, fresh: true };
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
      running_turn: running?.turn_id ?? null,
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
