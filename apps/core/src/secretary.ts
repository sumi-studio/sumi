import { jsonEqual, scrubJson, truncateText } from "./json.ts";
import type { ChatMessage, ModelProvider, ToolCall } from "./provider.ts";
import { FencedError, type StateClient, StateError } from "./state-client.ts";
import { toolSpecs } from "./tools.ts";
import type {
  CommitRequest,
  Decision,
  Event,
  Input,
  Turn,
  TurnPlan,
  WriterLease,
} from "./types.ts";

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
 * Guarantees delegated to the state service: writer fencing, plan-bound
 * idempotent claims, atomic internal tool effects, durable results before
 * success is observable. This class holds no canonical state — killing it
 * at any point is safe; the next generation recovers.
 *
 * Crash-safety scope (F1 durable plans): the model's decision is persisted
 * for the input before any of its effects run. A retried attempt continues
 * the recorded plan — the model is never re-consulted — and claims are
 * bound to plan positions server-side, so a retried turn can neither
 * duplicate a committed effect nor receive another request's receipt.
 * Scope: one plan per input, single pass — the recorded decision is not
 * revised mid-turn (plan revisions are a later contract).
 */
export class Secretary {
  private lease: WriterLease | null = null;
  private running = false;
  private inFlight: AbortController | null = null;
  private lastDispatch = 0;
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
      const now = Date.now();
      if (now - this.lastDispatch >= this.cfg.scheduleEveryMs) {
        this.lastDispatch = now;
        const fired = await state.dispatchSchedules(
          personaId,
          gen,
          new Date(now),
        );
        if (fired.length) this.log("schedules fired", { count: fired.length });
      }
      const turnId = this.cfg.idgen();
      const { turn, input, context, plan } = await state.loadTurn(
        personaId,
        gen,
        turnId,
        this.cfg.contextLimit,
      );
      if (!turn || !input) return "idle";
      await this.runTurn(turn, input, context, plan);
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
   * Process one claimed turn. The durable plan is the authority: if the
   * input already has a recorded decision the model is never consulted —
   * this attempt executes the stored plan's calls. Otherwise the model
   * streams once and the decision is persisted via savePlan before any
   * effect runs. Failures before commit leave the turn running for a
   * future generation to recover — results are durable before success is
   * observable.
   */
  private async runTurn(
    turn: Turn,
    input: Input,
    context: Event[],
    plan: TurnPlan | null,
  ): Promise<void> {
    const { state, personaId } = this.cfg;
    const gen = turn.generation;
    this.inFlight = new AbortController();
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

    const decision =
      plan?.plan ?? (await this.decide(turn, input, context, events));
    if (!decision) return; // failure already committed inside decide()

    const results: {
      tool: string;
      call_id: string;
      result: unknown;
      replayed: boolean;
    }[] = [];
    for (let i = 0; i < decision.calls.length; i++) {
      const call = decision.calls[i];
      if (!call) continue;
      const callId = call.call_id ?? "";
      let claim: Awaited<ReturnType<StateClient["claimOperation"]>>;
      try {
        claim = await state.claimOperation(personaId, gen, {
          operationId: `${turn.turn_id}:op:${i}`,
          turnId: turn.turn_id,
          tool: call.tool,
          callIndex: i,
          request: call.request,
        });
      } catch (e) {
        if (e instanceof FencedError) throw e;
        const msg = e instanceof Error ? e.message : String(e);
        if (e instanceof StateError && (e.status === 400 || e.status === 422)) {
          // Definite rejection (bad request / unsupported tool): record it as
          // the tool result rather than abandoning the whole turn.
          results.push({
            tool: call.tool,
            call_id: callId,
            result: { error: msg },
            replayed: false,
          });
          events.push({
            kind: "tool_result",
            payload: { tool: call.tool, call_id: callId, error: msg },
          });
          continue;
        }
        if (e instanceof StateError && e.status === 409) {
          // Plan-boundary violation: missing plan, off-plan position, or a
          // replay that diverges from the committed effect. Fail loudly —
          // replaying a stored receipt or re-executing would both be wrong.
          await this.failDivergent(turn, events, msg);
          return;
        }
        // Anything else (5xx, network, auth outage) is transient: leave the
        // turn running for a future generation to recover and retry.
        throw e;
      }
      const { operation, fresh } = claim;
      if (!fresh && !jsonEqual(operation.request, call.request)) {
        // Defense in depth: a store that replays a receipt for a different
        // request than the plan position's is detected client-side too.
        await this.failDivergent(
          turn,
          events,
          `${call.tool} request differs from committed operation ${operation.operation_id}`,
        );
        return;
      }
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
          tool: call.tool,
          call_id: callId,
          result: { error: "uncompleted operation" },
          replayed: false,
        });
        continue;
      }
      results.push({
        tool: call.tool,
        call_id: callId,
        result: operation.response,
        replayed: !fresh,
      });
      events.push({
        kind: "tool_call",
        payload: { tool: call.tool, call_id: callId, request: call.request },
      });
      events.push({
        kind: "tool_result",
        payload: {
          tool: call.tool,
          call_id: callId,
          response: operation.response,
          replayed: !fresh,
        },
      });
    }

    events.push({
      kind: "assistant_message",
      payload: { text: decision.text },
    });
    await this.commitTurnFinal(turn, {
      outcome: "complete",
      events,
      output: { text: decision.text, tool_results: results },
      usage: decision.usage,
    });
    this.log("turn committed", {
      turn_id: turn.turn_id,
      input_id: input.input_id,
      tools: decision.calls.length,
      replayed_plan: plan !== null,
    });
  }

  /**
   * Consult the model once and persist its decision via savePlan before any
   * effect runs. Returns the stored decision, or null when the turn was
   * already resolved (model failure committed retryable, or a conflicting
   * stored plan committed non-retryable). A crash before savePlan loses
   * nothing — no effect could have committed, so re-planning is safe.
   */
  private async decide(
    turn: Turn,
    input: Input,
    context: Event[],
    events: { kind: string; payload: Record<string, unknown> }[],
  ): Promise<Decision | null> {
    const { state, personaId } = this.cfg;
    const gen = turn.generation;
    let text = "";
    const calls: ToolCall[] = [];
    let usage: Record<string, unknown> = {};
    const messages = assemble(context, input);
    try {
      for await (const ev of this.cfg.provider.stream({
        personaId,
        turnId: turn.turn_id,
        messages,
        tools: toolSpecs(),
        signal: this.inFlight?.signal,
      })) {
        if (ev.type === "text") text += ev.delta;
        else if (ev.type === "tool_call") calls.push(ev.call);
        else usage = ev.usage;
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      await this.commitTurnFinal(turn, {
        outcome: "fail",
        retryable: true,
        // Bound at the source too: a provider error can be megabytes, and
        // the first commit upload should never carry that onto a
        // memory-limited host. The truncation marker stays in the record.
        error: `model: ${truncateText(msg, RECORDED_ERROR_BYTES)}`,
        events,
      });
      // Bound the log line too — a provider error can be megabytes.
      this.log("turn failed at model", {
        turn_id: turn.turn_id,
        error: truncateText(msg, 4 * 1024),
      });
      return null;
    }
    try {
      const saved = await state.savePlan(personaId, gen, {
        turnId: turn.turn_id,
        text,
        calls: calls.map((c) => ({
          call_id: c.id,
          tool: c.name,
          request: c.arguments,
        })),
        usage,
      });
      // Always execute the stored row — a lost-response resend returns the
      // identical plan; a conflict never silently substitutes.
      return saved.plan.plan;
    } catch (e) {
      if (e instanceof FencedError) throw e;
      if (e instanceof StateError && e.status === 409) {
        // A different plan is already recorded for this input. Fail loudly
        // rather than executing off the wrong decision.
        const msg = e instanceof Error ? e.message : String(e);
        await this.failDivergent(turn, events, `conflicting plan: ${msg}`);
        return null;
      }
      if (e instanceof StateError && e.status === 400) {
        // The decision itself cannot be recorded (deterministic rejection —
        // e.g. a NUL jsonb cannot hold). Retrying savePlan or re-planning
        // can never succeed, so commit a recorded non-retryable failure
        // rather than leaving the input to poison the queue.
        const msg = e instanceof Error ? e.message : String(e);
        await this.failDivergent(
          turn,
          events,
          `decision could not be recorded: ${msg}`,
        );
        return null;
      }
      // Transient: no effect committed yet — leave the turn running for
      // recovery to retry the save (idempotent on identical body).
      throw e;
    }
  }

  /**
   * The state service reported a durable-plan boundary violation — a
   * conflicting plan write, a missing plan, an off-plan claim, or a replay
   * diverging from a committed effect. Commit a non-retryable failure: the
   * input is done, the journal keeps the events so far, and the turn row
   * records why — no fabricated tool result, no poison requeue loop, no
   * duplicate effect.
   */
  private async failDivergent(
    turn: Turn,
    events: { kind: string; payload: Record<string, unknown> }[],
    msg: string,
  ): Promise<void> {
    await this.commitTurnFinal(turn, {
      outcome: "fail",
      retryable: false,
      error: `diverged retry conflicts with committed operation: ${msg}`,
      events,
    });
    this.log("turn failed on plan divergence", {
      turn_id: turn.turn_id,
      error: msg,
    });
  }

  /**
   * Commit a turn so a deterministic rejection cannot strand the input.
   *
   * A commit payload can itself be un-storable: model output or error
   * text containing NUL (jsonb rejects it) or a body over the request
   * limit both surface as a deterministic 400. Retrying the identical
   * commit can never succeed, and abandoning the turn leaves the input
   * claimed by a running turn that every recovery cycle fails to
   * finalize — a permanent poison loop.
   *
   * On 400 the commit is retried once with un-storable bytes scrubbed
   * and — for a complete outcome — downgraded to a recorded
   * non-retryable failure rather than a fabricated success (effects
   * already applied stay recorded; they are never un-committed). If the
   * scrubbed payload is still rejected, a minimal failure commit
   * finalizes the turn and honestly notes that the events could not be
   * stored. Anything else (5xx, network) is transient and propagates —
   * the running turn is recovered and retried; storage unavailability
   * must never become a fabricated record.
   */
  private async commitTurnFinal(turn: Turn, req: CommitRequest): Promise<void> {
    const { state, personaId } = this.cfg;
    const commit = (r: CommitRequest) =>
      state.commitTurn(personaId, turn.turn_id, turn.generation, r);
    let rejected: StateError | null = null;
    try {
      await commit(req);
      return;
    } catch (e) {
      if (!(e instanceof StateError && e.status === 400)) throw e;
      rejected = e;
    }
    // error is the only field surviving every tier, so it must be bounded
    // or a >body-limit provider/tool error re-creates the strand the
    // fallback exists to remove. The record keeps the reason — including
    // the server's own rejection ("read body" vs a data rejection) — and
    // says explicitly that it was truncated. Everything goes through the
    // same NUL scrub: the original text may be what made the commit
    // un-storable in the first place.
    const why = truncateText(scrubJson(rejected.message) as string, 512);
    const detail = truncateText(
      scrubJson(req.error ?? req.outcome) as string,
      RECORDED_ERROR_BYTES,
    );
    const msg = `commit rejected deterministically (${why}): ${detail}`;
    const scrubbed: CommitRequest = {
      outcome: "fail",
      // A fail+retryable commit (e.g. a provider error whose message was
      // un-storable) keeps its retryable disposition — the input still
      // deserves the retry. Only an un-storable *complete* downgrade is
      // terminal, since its output cannot be honestly recorded.
      retryable: req.outcome === "fail" && req.retryable === true,
      events: (req.events ?? []).map((e) => ({
        kind: e.kind,
        payload: scrubJson(e.payload) as Record<string, unknown>,
      })),
      error: msg,
    };
    try {
      await commit(scrubbed);
      this.log("turn committed as scrubbed failure", {
        turn_id: turn.turn_id,
      });
      return;
    } catch (e) {
      if (!(e instanceof StateError && e.status === 400)) throw e;
      rejected = e;
    }
    // Last resort: both rejection reasons sit ahead of the bounded detail
    // so neither is truncated away, and a retryable disposition survives —
    // a transient failure whose record could not fit still deserves the
    // retry. Only an un-storable *complete* downgrade is terminal.
    const why2 = truncateText(scrubJson(rejected.message) as string, 512);
    await commit({
      outcome: "fail",
      retryable: req.outcome === "fail" && req.retryable === true,
      events: [],
      error: `commit rejected deterministically (${why}; then ${why2}): ${detail}; original commit events could not be stored`,
    });
  }
}

// Recorded-error budget: a persisted failure only needs the reason, not
// megabytes. Kept far below the state service's 1 MiB body limit so the
// minimal fallback commit is always storable.
const RECORDED_ERROR_BYTES = 8 * 1024;
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
        // Flattened to assistant text: a bare role:"tool" message with no
        // preceding assistant tool_calls is rejected by chat-completions
        // providers. The journal keeps the structured record; the model gets
        // the result inline.
        messages.push({
          role: "assistant",
          content: `[tool ${String(p.tool ?? "?")}] ${JSON.stringify(
            p.response ?? (p.error ? { error: p.error } : {}),
          )}`,
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
