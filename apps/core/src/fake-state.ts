import { jsonEqual } from "./json.ts";
import { FencedError, StateError, type StateClient } from "./state-client.ts";
import type {
  CommitRequest,
  Event,
  Input,
  LoadResult,
  Operation,
  OutboxEntry,
  PersonaState,
  RecoverResult,
  Schedule,
  Turn,
  WriterLease,
} from "./types.ts";

/**
 * In-memory StateClient implementing the same contract semantics as the Go
 * service — fencing, idempotent claims, atomic internal effects, recovery —
 * for fast unit tests. NOT canonical storage; real-PG verification lives in
 * the Go suite and the e2e harness.
 */
export class FakeState implements StateClient {
  personas = new Map<
    string,
    { human_id: string | null; display_name: string; created_at: string }
  >();
  /** Live or expired lease row per persona — release never deletes (Go B1 fix). */
  leases = new Map<string, WriterLease>();
  /** Monotonic fencing generation per persona; never recycled. */
  private generations = new Map<string, number>();
  inputs: Input[] = [];
  turns = new Map<string, Turn>();
  eventLog: Event[] = [];
  ops = new Map<string, Operation>(); // key: persona|tool|idem
  schedules = new Map<string, Schedule>();
  outboxEntries: OutboxEntry[] = [];
  /** First commit request per turn — replay comparison (commit_request). */
  private commits = new Map<string, CommitRequest>();
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

  addPersona(personaId: string, displayName = personaId) {
    this.personas.set(personaId, {
      human_id: null,
      display_name: displayName,
      created_at: new Date().toISOString(),
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
      };
    }
    const input = this.inputs.find(
      (i) => i.persona_id === persona && i.status === "queued",
    );
    if (!input) return { turn: null, input: null, context: [] };
    input.status = "claimed";
    input.claimed_generation = generation;
    input.turn_id = turnId;
    const turn: Turn = {
      persona_id: persona,
      turn_id: turnId,
      input_id: input.input_id,
      generation,
      attempt:
        [...this.turns.values()].filter((t) => t.input_id === input.input_id)
          .length + 1,
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
    };
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
    } else {
      turn.status = "failed";
      turn.error = req.error ?? "failed";
      if (req.retryable) {
        input.status = "queued";
        input.claimed_generation = null;
        input.turn_id = null;
      } else {
        input.status = "done";
        input.done_at = new Date().toISOString();
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
      idempotencyKey: string;
      request: Record<string, unknown>;
    },
  ): Promise<{ operation: Operation; fresh: boolean }> {
    // Unregistered tools are rejected at the boundary (Go ErrUnknownTool →
    // 400), before the fence check — a dangling 'running' op is never
    // recorded for a tool no executor can finish.
    if (op.tool !== "schedule.set" && op.tool !== "journal.note") {
      throw new StateError(400, `unknown tool: ${op.tool}`);
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
    const k = this.key(persona, op.tool, op.idempotencyKey);
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
      if (existing.status === "running" && existing.claimed_generation !== generation) {
        existing.claimed_generation = generation;
        existing.turn_id = op.turnId;
        return { operation: existing, fresh: true };
      }
      return { operation: existing, fresh: false };
    }
    const operation: Operation = {
      persona_id: persona,
      operation_id: op.operationId,
      turn_id: op.turnId,
      tool: op.tool,
      idempotency_key: op.idempotencyKey,
      request: op.request,
      status: "done",
      response: null,
      claimed_generation: generation,
      created_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
    };
    if (op.tool === "schedule.set") {
      const sid = (op.request.schedule_id as string) ?? `sch-${Date.now()}`;
      const sch: Schedule = this.schedules.get(`${persona}|${sid}`) ?? {
        persona_id: persona,
        schedule_id: sid,
        wake_at: String(op.request.wake_at),
        payload: (op.request.payload as Record<string, unknown>) ?? {},
        miss_policy: "fire_late",
        status: "pending",
        claimed_generation: null,
        created_at: new Date().toISOString(),
        fired_at: null,
      };
      this.schedules.set(`${persona}|${sid}`, sch);
      operation.response = { schedule: sch };
    } else {
      const ev: Event = {
        persona_id: persona,
        seq: ++this.seq,
        turn_id: op.turnId,
        kind: "note",
        payload: { text: op.request.text },
        created_at: new Date().toISOString(),
      };
      this.eventLog.push(ev);
      operation.response = { seq: ev.seq, kind: "note" };
    }
    this.ops.set(k, operation);
    return { operation, fresh: true };
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
}
