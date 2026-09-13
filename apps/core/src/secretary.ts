import type { ChatMessage, ModelProvider, ToolCall } from "./provider.ts";
import { FencedError, type StateClient } from "./state-client.ts";
import { toolSpecs } from "./tools.ts";
import type { Event, Input, Turn, WriterLease } from "./types.ts";

export interface SecretaryConfig {
  personaId: string;
  /** Unique per boot — distinguishes this writer's lease. */
  holderId: string;
  state: StateClient;
  provider: ModelProvider;
  /** Lease TTL; renewed while running. */
  leaseTtlMs: number;
  /** How often to renew the lease (<< leaseTtlMs). */
  renewEveryMs: number;
  /** Journal tail length loaded as model context per turn. */
  contextLimit: number;
  /** Idle poll interval while waiting for inputs. */
  pollIntervalMs: number;
  /** Schedule dispatch cadence. */
  scheduleEveryMs: number;
  idgen: () => string;
  log?: (msg: string, fields?: Record<string, unknown>) => void;
}

export type StepResult = "turn" | "idle" | "stopped";

/**
 * One continuing secretary life. Boot = acquire writer lease + recover
 * abandoned work, then loop: dispatch due schedules → claim input + begin
 * turn → stream model → ledger tool calls → commit turn + outbox.
 *
 * Guarantees delegated to the state service: writer fencing, idempotent
 * claims, atomic internal tool effects, durable results before success is
 * observable. This class holds no canonical state — killing it at any point
 * is safe; the next generation recovers.
 */
export class Secretary {
  private lease: WriterLease | null = null;
  private running = false;
  private inFlight: AbortController | null = null;
  private readonly log: (msg: string, fields?: Record<string, unknown>) => void;
  private readonly cfg: SecretaryConfig;

  constructor(cfg: SecretaryConfig) {
    this.cfg = cfg;
    this.log = cfg.log ?? (() => {});
  }

  get generation(): number | null {
    return this.lease?.generation ?? null;
  }

  /** Acquire the writer lease and recover whatever the last holder left. */
  async start(): Promise<void> {
    const { state, personaId, holderId, leaseTtlMs } = this.cfg;
    this.lease = await state.acquireWriter(personaId, holderId, leaseTtlMs);
    const gen = this.lease.generation;
    const rec = await state.recover(personaId, gen);
    this.log("writer acquired", {
      generation: gen,
      interrupted_turns: rec.interrupted_turns.length,
      requeued_inputs: rec.requeued_inputs.length,
    });
    this.running = true;
  }

  /** One unit of work: fire due schedules, then take one turn if an input waits. */
  async step(): Promise<StepResult> {
    if (!this.running || !this.lease) return "stopped";
    const gen = this.lease.generation;
    const { state, personaId } = this.cfg;
    try {
      const fired = await state.dispatchSchedules(personaId, gen, new Date());
      if (fired.length) this.log("schedules fired", { count: fired.length });
      const turnId = this.cfg.idgen();
      const { turn, input, context } = await state.loadTurn(
        personaId,
        gen,
        turnId,
        this.cfg.contextLimit,
      );
      if (!turn || !input) return "idle";
      await this.runTurn(turn, input, context);
      return "turn";
    } catch (e) {
      if (e instanceof FencedError) {
        this.log("lost writer fence; stopping", { generation: gen });
        this.running = false;
        this.lease = null;
        return "stopped";
      }
      throw e;
    }
  }

  /** Long-running loop; resolves when stopped (signal, fence loss, error). */
  async run(signal?: AbortSignal): Promise<void> {
    if (!this.running) await this.start();
    const renewEvery = Math.max(500, this.cfg.renewEveryMs);
    let lastRenew = 0;
    while (this.running && !signal?.aborted) {
      const now = Date.now();
      if (this.lease && now - lastRenew >= renewEvery) {
        lastRenew = now;
        try {
          this.lease = await this.cfg.state.renewWriter(
            this.cfg.personaId,
            this.cfg.holderId,
            this.lease.generation,
            this.cfg.leaseTtlMs,
          );
        } catch (e) {
          if (e instanceof FencedError) break;
          this.log("lease renewal failed", { error: String(e) });
        }
      }
      const result = await this.step();
      if (result === "idle") {
        await sleep(this.cfg.pollIntervalMs, signal);
      } else if (result === "stopped") {
        break;
      }
    }
    await this.shutdown();
  }

  /** Drain in-flight work opportunity and release the lease. */
  async stop(): Promise<void> {
    this.running = false;
    this.inFlight?.abort();
    await this.shutdown();
  }

  private async shutdown(): Promise<void> {
    if (!this.lease) return;
    try {
      await this.cfg.state.releaseWriter(
        this.cfg.personaId,
        this.cfg.holderId,
        this.lease.generation,
      );
      this.log("writer released", { generation: this.lease.generation });
    } catch (e) {
      this.log("lease release failed (will expire)", { error: String(e) });
    }
    this.lease = null;
    this.running = false;
  }

  /**
   * Process one claimed turn: stream the model, ledger every tool call,
   * then commit events + outcome + outbox in one transaction. Failures
   * before commit leave the turn running for a future generation to
   * recover — results are durable before success is observable.
   */
  private async runTurn(
    turn: Turn,
    input: Input,
    context: Event[],
  ): Promise<void> {
    const { state, personaId } = this.cfg;
    const gen = turn.generation;
    this.inFlight = new AbortController();
    const messages = assemble(context, input);
    let text = "";
    const calls: ToolCall[] = [];
    let usage: Record<string, unknown> = {};
    const events: { kind: string; payload: Record<string, unknown> }[] = [
      {
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
      },
    ];
    try {
      for await (const ev of this.cfg.provider.stream({
        personaId,
        turnId: turn.turn_id,
        messages,
        tools: toolSpecs(),
        signal: this.inFlight.signal,
      })) {
        if (ev.type === "text") text += ev.delta;
        else if (ev.type === "tool_call") calls.push(ev.call);
        else usage = ev.usage;
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      await state.commitTurn(personaId, turn.turn_id, gen, {
        outcome: "fail",
        retryable: true,
        error: `model: ${msg}`,
        events,
      });
      this.log("turn failed at model", { turn_id: turn.turn_id, error: msg });
      return;
    }

    const results: {
      tool: string;
      call_id: string;
      result: unknown;
      replayed: boolean;
    }[] = [];
    for (let i = 0; i < calls.length; i++) {
      const call = calls[i];
      if (!call) continue;
      // Deterministic per input+position: a retried turn replays stored
      // results instead of re-executing effects.
      const idempotencyKey = `${input.input_id}:tool:${i}`;
      let claim: Awaited<ReturnType<StateClient["claimOperation"]>>;
      try {
        claim = await state.claimOperation(personaId, gen, {
          operationId: `${turn.turn_id}:op:${i}`,
          turnId: turn.turn_id,
          tool: call.name,
          idempotencyKey,
          request: call.arguments,
        });
      } catch (e) {
        if (e instanceof FencedError) throw e;
        // Unsupported/rejected tool: record the failure as the tool result
        // rather than abandoning the whole turn.
        const msg = e instanceof Error ? e.message : String(e);
        results.push({
          tool: call.name,
          call_id: call.id,
          result: { error: msg },
          replayed: false,
        });
        events.push({
          kind: "tool_result",
          payload: { tool: call.name, call_id: call.id, error: msg },
        });
        continue;
      }
      const { operation, fresh } = claim;
      if (operation.status === "running") {
        // A prior attempt crashed after claiming but before the receipt was
        // recorded. For internal tools this cannot happen (effect+receipt are
        // one tx) — running state implies an external executor we don't have
        // yet; fail the op rather than silently re-run an ambiguous effect.
        await state.completeOperation(
          personaId,
          operation.operation_id,
          gen,
          {
            error:
              "operation left running by prior attempt; external execution not implemented in this slice",
          },
          true,
        );
        results.push({
          tool: call.name,
          call_id: call.id,
          result: { error: "uncompleted operation" },
          replayed: false,
        });
        continue;
      }
      results.push({
        tool: call.name,
        call_id: call.id,
        result: operation.response,
        replayed: !fresh,
      });
      events.push({
        kind: "tool_call",
        payload: { tool: call.name, call_id: call.id, request: call.arguments },
      });
      events.push({
        kind: "tool_result",
        payload: {
          tool: call.name,
          call_id: call.id,
          response: operation.response,
          replayed: !fresh,
        },
      });
    }

    events.push({ kind: "assistant_message", payload: { text } });
    await state.commitTurn(personaId, turn.turn_id, gen, {
      outcome: "complete",
      events,
      output: { text, tool_results: results },
      usage,
    });
    this.log("turn committed", {
      turn_id: turn.turn_id,
      input_id: input.input_id,
      tools: calls.length,
    });
  }
}

const SYSTEM =
  "You are a personal secretary — one continuing life across restarts, not a stateless handler. " +
  "Your journal is your durable memory. You may schedule.set future wake-ups and journal.note what matters. " +
  "Keep replies brief and honest; do not claim abilities you do not have.";

/** Assemble model messages from the journal tail plus the current input. */
export function assemble(context: Event[], input: Input): ChatMessage[] {
  const messages: ChatMessage[] = [{ role: "system", content: SYSTEM }];
  for (const ev of context) {
    const p = ev.payload;
    switch (ev.kind) {
      case "input_received": {
        const who =
          p.actor_kind === "schedule"
            ? "[scheduled wake]"
            : `[${String(p.actor_kind)}]`;
        messages.push({
          role: "user",
          content: `${who} ${String(p.text ?? "")}`,
        });
        break;
      }
      case "assistant_message":
        messages.push({ role: "assistant", content: String(p.text ?? "") });
        break;
      case "note":
        messages.push({
          role: "assistant",
          content: `[note] ${String(p.text ?? "")}`,
        });
        break;
      case "tool_result":
        messages.push({
          role: "tool",
          toolCallId: String(p.call_id ?? ""),
          name: String(p.tool ?? ""),
          content: JSON.stringify(p.response ?? {}),
        });
        break;
      default:
        break;
    }
  }
  const text =
    typeof input.payload.text === "string"
      ? input.payload.text
      : JSON.stringify(input.payload);
  const who =
    input.actor_kind === "schedule"
      ? "[scheduled wake]"
      : `[${input.actor_kind}]`;
  messages.push({ role: "user", content: `${who} ${text}` });
  return messages;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      resolve();
    });
  });
}
