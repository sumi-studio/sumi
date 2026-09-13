import type { StateClient } from "./state-client.ts";
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
  lease: WriterLease | null = null;
  inputs: Input[] = [];
  turns = new Map<string, Turn>();
  eventLog: Event[] = [];
  ops = new Map<string, Operation>(); // key: persona|tool|idem
  schedules = new Map<string, Schedule>();
  outboxEntries: OutboxEntry[] = [];
  private seq = 0;
  private outboxSeq = 0;

  private key(persona: string, tool: string, idem: string) {
    return `${persona}|${tool}|${idem}`;
  }

  private mustHold(persona: string, generation: number) {
    if (
      !this.lease ||
      this.lease.persona_id !== persona ||
      this.lease.generation !== generation
    ) {
      throw new Error("writer generation fenced");
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
    if (this.lease && new Date(this.lease.expires_at).getTime() > now) {
      throw new Error("writer lease already held");
    }
    this.lease = {
      persona_id: persona,
      generation: (this.lease?.generation ?? 0) + 1,
      holder_id: holder,
      acquired_at: new Date(now).toISOString(),
      expires_at: new Date(now + ttlMs).toISOString(),
    };
    return this.lease;
  }

  async renewWriter(
    persona: string,
    holder: string,
    generation: number,
    ttlMs: number,
  ): Promise<WriterLease> {
    this.mustHold(persona, generation);
    const held = this.lease;
    if (!held || held.holder_id !== holder)
      throw new Error("writer lease held by another holder");
    this.lease = {
      ...held,
      expires_at: new Date(Date.now() + ttlMs).toISOString(),
    };
    return this.lease;
  }

  async releaseWriter(
    persona: string,
    holder: string,
    generation: number,
  ): Promise<void> {
    if (
      this.lease?.persona_id === persona &&
      this.lease.holder_id === holder &&
      this.lease.generation === generation
    ) {
      this.lease = null;
    }
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
    if (turn.status !== "running") return turn; // replay: durable result already stands
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
    this.mustHold(persona, generation);
    const k = this.key(persona, op.tool, op.idempotencyKey);
    const existing = this.ops.get(k);
    if (existing) return { operation: existing, fresh: false };
    if (op.tool !== "schedule.set" && op.tool !== "journal.note") {
      throw new Error(`unsupported tool: ${op.tool}`);
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
      lease: this.lease?.persona_id === persona ? this.lease : null,
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
