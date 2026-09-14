import { jsonEqual } from "./json.ts";
import {
  ModelError,
  type ChatMessage,
  type ModelProvider,
  type ToolCall,
} from "./provider.ts";
import {
  FencedError,
  type StateClient,
  StateError,
  UnauthorizedError,
} from "./state-client.ts";
import { toolSpecs } from "./tools.ts";
import type {
  CommitRequest,
  Decision,
  Event,
  EventInput,
  Input,
  Json,
  PlanCall,
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
  /** Lease TTL; renewed while running — including inside an in-flight turn. */
  leaseTtlMs: number;
  /** How often to renew the lease (<< leaseTtlMs). */
  renewEveryMs: number;
  /** Journal tail length loaded as model context per turn. */
  contextLimit: number;
  /** Idle poll interval while waiting for inputs. */
  pollIntervalMs: number;
  /** Schedule dispatch cadence. */
  scheduleEveryMs: number;
  /**
   * Max turns (attempts) one input may consume before it is marked
   * done-failed — a plan that keeps hitting transient errors cannot burn
   * attempts forever or starve the inputs queued behind it. The cap only
   * fires once the provider-retry budget has also elapsed; within the
   * budget, attempts continue so a short provider outage is survived.
   * Default 5.
   */
  maxAttempts?: number;
  /**
   * Wall-clock budget for retrying transient provider failures, measured
   * from the input's submission (`created_at`) so it survives restarts
   * and is identical across hosts. Provider rate limits and short
   * incidents resolve in seconds to tens of minutes; the default 30 min
   * rides out ordinary outages without discarding the request, and still
   * bounds a provider that never recovers. Provider-supplied
   * Retry-After pacing is honored per attempt on top of the durable
   * backoff. Default 30 minutes.
   */
  providerRetryBudgetMs?: number;
  /**
   * Max model consultations (rounds) per turn. A round that ends with tool
   * calls feeds its committed results back into the next consultation; a
   * round with zero calls is final and its text is the reply. A model that
   * never stops calling tools hits this cap and the turn fails
   * non-retryable. Default 6.
   */
  maxToolRounds?: number;
  idgen: () => string;
  log?: (msg: string, fields?: Record<string, unknown>) => void;
}

export type StepResult = "turn" | "idle" | "stopped";

/** Default wall-clock budget for transient provider retries (F1). */
const PROVIDER_RETRY_BUDGET_MS = 30 * 60_000;

/**
 * How long the input has been actively worked on: wall time since
 * submission minus the durably recorded time it spent parked on human
 * approval decisions. A person's thinking time is not model failure —
 * the state service accumulates it in waited_ms on every requeue, so the
 * budget is identical across restarts and a long wait cannot exhaust the
 * provider retry window (repair F4).
 */
function activeAgeMs(input: Input): number {
  let active = Date.now() - Date.parse(input.created_at) - (input.waited_ms ?? 0);
  // A still-waiting input cannot be claimed — but a store that exposes
  // waiting_since without having requeued yet is counted honestly too.
  if (input.waiting_since) {
    active -= Date.now() - Date.parse(input.waiting_since);
  }
  return active;
}

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
 * Crash-safety scope (F1 durable plans): each round's decision is persisted
 * for the input before any of that round's effects run. A retried attempt
 * continues the recorded rounds — recorded rounds are never re-consulted —
 * and claims are bound to flat plan positions server-side, so a retried
 * turn can neither duplicate a committed effect nor receive another
 * request's receipt. Multi-round scope: a round ending with tool calls is
 * followed by a new recorded round fed by the committed results; a round
 * with zero calls is final and its text is the reply.
 */
export class Secretary {
  private lease: WriterLease | null = null;
  private running = false;
  private inFlight: AbortController | null = null;
  private lastDispatch = 0;
  private poison: { turnId: string; count: number } | null = null;
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
    if (this.running && this.lease) {
      // Already running (e.g. a workerd drain resumed after a mid-turn
      // error): continue the same generation — re-acquiring would bump
      // it, interrupt the in-flight turn, and spend an attempt on a
      // bookkeeping error. The alarm cadence can exceed the TTL, so the
      // held lease must be renewed, not just reused.
      try {
        this.lease = await state.renewWriter(
          personaId,
          holderId,
          this.lease.generation,
          leaseTtlMs,
        );
        return;
      } catch (e) {
        if (!(e instanceof FencedError)) throw e;
        this.running = false;
        this.lease = null;
      }
    }
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
        const fired = await state.dispatchSchedules(personaId, gen, new Date(now));
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
      const maxAttempts = this.cfg.maxAttempts ?? 5;
      const budgetMs =
        this.cfg.providerRetryBudgetMs ?? PROVIDER_RETRY_BUDGET_MS;
      const withinBudget = activeAgeMs(input) < budgetMs;
      if (turn.attempt > maxAttempts && !withinBudget) {
        // Attempt cap (CR3-N1): an input that has already consumed its
        // allowance — model failures, transient claim errors, a poison
        // plan — is done-failed here, before any further model consult or
        // effect claim, so it cannot consume indefinitely or starve the
        // inputs queued behind it. The cap only fires after the provider
        // retry budget has elapsed: within it, a transient provider
        // outage keeps retrying rather than discarding the request
        // (fresh-review F1).
        await this.commitTurnFinal(turn, {
          outcome: "fail",
          retryable: false,
          error: `attempt cap ${maxAttempts} exceeded for input`,
          events: [inputReceivedEvent(input, turn)],
        });
        this.log("input abandoned at attempt cap", {
          turn_id: turn.turn_id,
          input_id: input.input_id,
          attempt: turn.attempt,
        });
        return "turn";
      }
      try {
        await this.runTurn(turn, input, context, plan);
        this.poison = null;
      } catch (e) {
        if (
          e instanceof FencedError ||
          e instanceof StateError ||
          isTransportError(e)
        ) {
          throw e; // genuine outage — run() backs off and retries
        }
        // An unknown failure while a turn is in flight — an invariant
        // violation or a corrupt response — would otherwise retry
        // forever at backoff pace without consuming attempts
        // (fresh-review F6). A few consecutive strikes on the same turn
        // resolve to an honest recorded failure instead of a silent loop.
        const msg = stripNul(e instanceof Error ? e.message : String(e));
        this.poison =
          this.poison?.turnId === turn.turn_id
            ? { turnId: turn.turn_id, count: this.poison.count + 1 }
            : { turnId: turn.turn_id, count: 1 };
        if (this.poison.count < 3) throw e;
        this.poison = null;
        await this.commitTurnFinal(turn, {
          outcome: "fail",
          retryable: false,
          error: `recurring internal error: ${truncateText(msg, RECORDED_ERROR_BYTES)}`,
          events: [inputReceivedEvent(input, turn)],
        });
        this.log("turn failed after recurring internal error", {
          turn_id: turn.turn_id,
          error: truncateText(msg, 4 * 1024),
        });
      }
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

  /**
   * True when a state-call failure is worth riding out inside the loop:
   * transport errors (service restart, PG blip, fetch timeouts) and
   * 5xx/429 responses. Auth and deterministic 4xx rejections are not
   * transient — retrying a wrong token or a contract violation only
   * burns the attempt budget. (Review-A F1.)
   */
  private isTransient(e: unknown): boolean {
    if (e instanceof FencedError || e instanceof UnauthorizedError) {
      return false;
    }
    if (e instanceof StateError) return e.status >= 500 || e.status === 429;
    // Anything without an HTTP status must still be a transport failure
    // (fetch TypeError, AbortError, DNS) — an unknown defect is not
    // transient and must not be retried forever in silence (F6).
    return isTransportError(e);
  }

  private transientBackoff(failures: number): number {
    return Math.min(30_000, 500 * 2 ** Math.min(failures, 6));
  }

  /** Long-running loop; resolves when stopped (signal, fence loss, error). */
  async run(signal?: AbortSignal): Promise<void> {
    // A transient state-API failure — service restart, PG connection blip
    // — must not kill the secretary: the canonical record is in PG and a
    // bounded-backoff retry is the same recovery an external supervisor
    // would drive, inside the loop. Cancellation still exits promptly;
    // fencing still stops the writer.
    let failures = 0;
    const backoff = () =>
      sleep(this.transientBackoff(failures), signal);
    if (!this.running) {
      let heldDeadline = 0;
      for (;;) {
        try {
          await this.start();
          break;
        } catch (e) {
          // An abort while waiting out a held lease is a clean stop,
          // not a failure — resolve rather than rethrowing the 409
          // (final-review NF4). shutdown() is a no-op without a lease.
          if (signal?.aborted) return;
          if (e instanceof StateError && e.status === 409) {
            // The lease is held by another holder. On an ordinary
            // restart (default holder is local-<pid>) that holder is our
            // own dead predecessor: wait out its TTL and acquire rather
            // than dying instantly — that was the whole point of riding
            // transient failures in-process (fresh-review F3). A live
            // holder keeps renewing past the deadline; then yield
            // honestly instead of stealing the life.
            if (!heldDeadline) {
              // Up to two TTLs: one full expiry window plus grace for
              // clock skew. A live holder renews inside one TTL, so a
              // lease still held past this window is genuinely owned.
              heldDeadline =
                Date.now() +
                this.cfg.leaseTtlMs +
                Math.min(10_000, this.cfg.leaseTtlMs);
            }
            if (Date.now() < heldDeadline) {
              failures++;
              this.log("writer lease held; waiting for expiry", {
                attempt: failures,
                error: String(e),
              });
              await sleep(
                Math.min(
                  this.transientBackoff(failures),
                  heldDeadline - Date.now(),
                ),
                signal,
              );
              continue;
            }
            // Deadline passed: one final acquire — a merely-expired
            // predecessor is gone by now, so this either succeeds or
            // confirms a live holder outlasted the window. Its errors
            // still go through classification (final-review NF3): a
            // transient state hiccup at this exact moment keeps backing
            // off like any other attempt instead of exiting; a renewed
            // 409 confirms the live holder and yields honestly.
            try {
              await this.start();
              break;
            } catch (e2) {
              if (signal?.aborted) return;
              if (!this.isTransient(e2)) throw e2;
              failures++;
              this.log("state service unavailable; retrying writer acquire", {
                attempt: failures,
                error: String(e2),
              });
              await backoff();
            }
            continue;
          }
          if (!this.isTransient(e)) throw e;
          failures++;
          this.log("state service unavailable; retrying writer acquire", {
            attempt: failures,
            error: String(e),
          });
          await backoff();
        }
      }
    }
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
      try {
        const result = await this.step();
        failures = 0;
        if (result === "idle") {
          await sleep(this.cfg.pollIntervalMs, signal);
        } else if (result === "stopped") {
          break;
        }
      } catch (e) {
        if (!this.running || signal?.aborted) break;
        if (!this.isTransient(e)) throw e;
        failures++;
        this.log("state call failed; backing off", {
          attempt: failures,
          backoff_ms: this.transientBackoff(failures),
          error: String(e),
        });
        await backoff();
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
   * Renew the writer lease while a turn is in flight (CR3-N2). Model
   * latency is unbounded relative to the lease TTL — without this, a slow
   * stream lets the lease expire mid-turn and the next writer's acquire
   * fences the in-flight work. Renewal failures other than fencing are
   * logged and retried on the next tick; losing the fence aborts the
   * in-flight provider call so the turn cannot outlive its ownership.
   */
  private renewDuringTurn(generation: number): () => void {
    const every = Math.max(50, this.cfg.renewEveryMs);
    const t = setInterval(() => {
      this.cfg.state
        .renewWriter(
          this.cfg.personaId,
          this.cfg.holderId,
          generation,
          this.cfg.leaseTtlMs,
        )
        .then((lease) => {
          this.lease = lease;
        })
        .catch((e: unknown) => {
          if (e instanceof FencedError) {
            this.log("lost writer fence mid-turn", { generation });
            this.running = false;
            this.lease = null;
            this.inFlight?.abort();
          } else {
            this.log("lease renewal failed mid-turn", { error: String(e) });
          }
        });
    }, every);
    (t as { unref?: () => void }).unref?.();
    return () => clearInterval(t);
  }

  /**
   * Process one claimed turn. The durable plan is the authority: every
   * recorded round is replayed verbatim (claims return stored receipts);
   * the model is consulted only for the first round not yet recorded, and
   * that round is persisted via savePlan before any of its effects run. A
   * round with zero calls is final — its text is the reply, informed by
   * the committed tool results fed back to the model. Failures before
   * commit leave the turn running for a future generation to recover.
   */
  private async runTurn(
    turn: Turn,
    input: Input,
    context: Event[],
    plan: TurnPlan | null,
  ): Promise<void> {
    const gen = turn.generation;
    const maxRounds = this.cfg.maxToolRounds ?? 6;
    this.inFlight = new AbortController();
    const stopRenewal = this.renewDuringTurn(gen);
    try {
      const events: EventInput[] = [inputReceivedEvent(input, turn)];
      const messages = assemble(context, input);
      // The stored plan is the authority — re-sync on every savePlan so a
      // plan that grew further in a lost prior attempt is executed as
      // recorded, never as this attempt would have decided it.
      let rounds: Decision[] = plan?.plan ?? [];
      const results: {
        tool: string;
        call_id: string;
        result: unknown;
        replayed: boolean;
      }[] = [];
      const usages: Json[] = [];

      for (let r = 0; ; r++) {
        let decision = rounds[r];
        if (!decision) {
          const outcome = await this.decide(turn, input, events, messages, r);
          // Failure committed inside decide(); a retryable one is paced
          // durably by the requeue's not_before backoff, so the loop is
          // free to serve other inputs immediately.
          if ("failed" in outcome) return;
          rounds = outcome.rounds;
          decision = rounds[r];
          if (!decision) throw new Error("stored plan missing saved round");
        }
        usages.push(decision.usage);
        if (decision.text) {
          events.push({
            kind: "assistant_message",
            payload: { text: decision.text, round: r },
          });
        }

        // Execute this round's calls at their flat positions across all
        // rounds — the durable effect identity is input_id + flat index.
        const roundResults: { call_id: string; tool: string; result: unknown }[] = [];
        for (const call of decision.calls) {
          const res = await this.executeCall(turn, call, results.length, events);
          if (res === null) return; // divergent — failure committed
          if (res === "awaited") {
            // A gated call parked: its planned operation and the pending
            // approval are durable, so the commit can wait on them. The
            // store moves the input to waiting and emits the request
            // notification; an authenticated decision requeues the input
            // and the next attempt resumes this exact call. The turn is
            // recorded awaiting — never auto-executed after a restart.
            // Only the request itself is journaled now: the resuming attempt
            // replays this plan and journals the whole turn once, so the
            // input and its reply are not recorded twice.
            await this.commitTurnFinal(turn, {
              outcome: "await",
              events: events.filter((e) => e.kind === "approval_requested"),
            });
            this.log("turn awaiting approval", {
              turn_id: turn.turn_id,
              input_id: input.input_id,
            });
            return;
          }
          const result = { call_id: call.call_id ?? "", tool: call.tool, ...res };
          results.push(result);
          roundResults.push(result);
        }

        if (decision.calls.length === 0) {
          // Final round: the reply text post-dates every committed effect.
          await this.commitTurnFinal(turn, {
            outcome: "complete",
            events,
            output: { text: decision.text, tool_results: results },
            usage: { rounds: usages },
          });
          this.log("turn committed", {
            turn_id: turn.turn_id,
            input_id: input.input_id,
            rounds: r + 1,
            tools: results.length,
            replayed_plan: plan !== null,
          });
          return;
        }

        if (r + 1 >= maxRounds) {
          // The model kept calling tools past the round cap. Its effects
          // are committed and journaled truthfully; the turn fails
          // non-retryable rather than consulting forever.
          await this.commitTurnFinal(turn, {
            outcome: "fail",
            retryable: false,
            error: `tool round cap ${maxRounds} reached`,
            events,
          });
          this.log("turn failed at tool round cap", {
            turn_id: turn.turn_id,
            input_id: input.input_id,
          });
          return;
        }

        // Feed this round back for the next consultation: the assistant
        // message carries its decided calls, then one tool message per
        // committed result. Tool-result content is deterministic across
        // attempts — replays read stored receipts.
        messages.push({
          role: "assistant",
          content: decision.text,
          toolCalls: decision.calls.map((c) => ({
            id: c.call_id ?? "",
            name: c.tool,
            route: c.route,
            arguments: c.request,
          })),
        });
        for (const res of roundResults) {
          messages.push({
            role: "tool",
            toolCallId: res.call_id,
            name: res.tool,
            content: JSON.stringify(res.result ?? {}),
          });
        }
      }
    } finally {
      stopRenewal();
      this.inFlight = null;
    }
  }

  /**
   * Execute one planned call at its flat position. Returns the result to
   * feed back to the model, "awaited" when the call parked behind a pending
   * human decision, or null when a plan-boundary failure was committed and
   * the turn is over.
   */
  private async executeCall(
    turn: Turn,
    call: PlanCall,
    flatIndex: number,
    events: EventInput[],
  ): Promise<{ result: unknown; replayed: boolean } | "awaited" | null> {
    const { state, personaId } = this.cfg;
    const gen = turn.generation;
    const callId = call.call_id ?? "";
    let claim: Awaited<ReturnType<StateClient["claimOperation"]>>;
    try {
      claim = await state.claimOperation(personaId, gen, {
        operationId: `${turn.turn_id}:op:${flatIndex}`,
        turnId: turn.turn_id,
        tool: call.tool,
        callIndex: flatIndex,
        request: call.request,
      });
    } catch (e) {
      if (e instanceof FencedError) throw e;
      const msg = stripNul(e instanceof Error ? e.message : String(e));
      if (e instanceof StateError && (e.status === 400 || e.status === 422)) {
        // Definite rejection (bad request / unsupported tool): record it as
        // the tool result rather than abandoning the whole turn. The
        // tool_call event still journals first — the journal shows the
        // decided call and its rejection symmetrically.
        events.push({
          kind: "tool_call",
          payload: { tool: call.tool, call_id: callId, request: call.request },
        });
        events.push({
          kind: "tool_result",
          payload: { tool: call.tool, call_id: callId, error: msg },
        });
        return { result: { error: msg }, replayed: false };
      }
      if (e instanceof StateError && e.status === 409) {
        // Plan-boundary violation: missing plan, off-plan position, or a
        // replay that diverges from the committed effect. Fail loudly —
        // replaying a stored receipt or re-executing would both be wrong.
        await this.failPermanent(
          turn,
          events,
          `diverged retry conflicts with committed operation: ${msg}`,
        );
        return null;
      }
      // Anything else (5xx, network, auth outage) is transient: leave the
      // turn running for a future generation to recover and retry.
      throw e;
    }
    const { operation, approval, fresh } = claim;
    if (operation.status === "awaiting_approval") {
      // Gated call: the store durably recorded the planned operation and a
      // pending approval before returning — no effect ran. Record exactly
      // what is waiting on the human, then park.
      events.push({
        kind: "approval_requested",
        payload: {
          tool: call.tool,
          call_id: callId,
          route: call.route,
          approval_id: approval?.approval_id ?? null,
          required_by: approval?.required_by ?? null,
          request: call.request,
        },
      });
      return "awaited";
    }
    if (!fresh && !jsonEqual(operation.request, call.request)) {
      // Defense in depth: a store that replays a receipt for a different
      // request than the plan position's is detected client-side too.
      await this.failPermanent(
        turn,
        events,
        `diverged retry: ${call.tool} request differs from committed operation ${operation.operation_id}`,
      );
      return null;
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
      return { result: { error: "uncompleted operation" }, replayed: false };
    }
    events.push({
      kind: "tool_call",
      payload: {
        tool: call.tool,
        call_id: callId,
        request: call.request,
        route: call.route,
      },
    });
    if (operation.status === "failed") {
      // Durable denial (or another finalized failure): the decided call
      // must not be silently retried or bypassed — the failure itself is
      // the result the model sees, journaled as denied.
      const err =
        operation.response !== null &&
        typeof operation.response === "object" &&
        "error" in operation.response
          ? String((operation.response as { error: unknown }).error)
          : "operation failed";
      events.push({
        kind: "tool_result",
        payload: {
          tool: call.tool,
          call_id: callId,
          error: err,
          denied: approval?.status === "denied" || undefined,
          replayed: !fresh,
        },
      });
      return { result: { error: err }, replayed: !fresh };
    }
    events.push({
      kind: "tool_result",
      payload: {
        tool: call.tool,
        call_id: callId,
        response: operation.response,
        replayed: !fresh,
      },
    });
    return { result: operation.response, replayed: !fresh };
  }

  /**
   * Consult the model for one round and persist its decision via savePlan
   * before any effect runs. Returns the stored plan (all rounds) or a
   * failure marker when the turn was already resolved — a model failure
   * committed retryable while attempts remain, or a conflicting stored
   * plan committed non-retryable. A crash before savePlan loses nothing —
   * no effect could have committed, so re-planning that round is safe.
   */
  private async decide(
    turn: Turn,
    input: Input,
    events: EventInput[],
    messages: ChatMessage[],
    round: number,
  ): Promise<{ rounds: Decision[] } | { failed: true; retryable: boolean }> {
    const { state, personaId } = this.cfg;
    const gen = turn.generation;
    let text = "";
    const calls: ToolCall[] = [];
    let usage: Record<string, unknown> = {};
    try {
      for await (const ev of this.cfg.provider.stream({
        personaId,
        turnId: turn.turn_id,
        round,
        messages,
        tools: toolSpecs(),
        signal: this.inFlight?.signal,
      })) {
        if (ev.type === "text") text += ev.delta;
        else if (ev.type === "tool_call") calls.push(ev.call);
        else usage = ev.usage;
      }
    } catch (e) {
      if (!this.running) throw e; // fence lost mid-stream — leave the turn
      // Provider error text is untrusted bytes: a poisoned message (e.g.
      // one containing NUL) must not make the failure itself unpersistable.
      const msg = stripNul(e instanceof Error ? e.message : String(e));
      const mErr = e instanceof ModelError ? e : null;
      // Deterministic provider rejections cannot be fixed by retrying —
      // they fail the input outright. Transient failures (5xx/429,
      // network, timeout, an incomplete stream) stay retryable for a
      // wall-clock budget measured from the input's submission — a
      // seconds-long provider outage must not lose a request (F1). The
      // attempt cap still bounds inputs that die before recording a
      // failure; committed-transient retries are bounded by time, and a
      // retryable failure leaves no partial journal — the next attempt
      // re-emits its full event set.
      const withinBudget =
        activeAgeMs(input) <
        (this.cfg.providerRetryBudgetMs ?? PROVIDER_RETRY_BUDGET_MS);
      const retryable =
        mErr !== null && !mErr.retryable ? false : withinBudget;
      await this.commitTurnFinal(turn, {
        outcome: "fail",
        retryable,
        // Bound at the source too: a provider error can be megabytes, and
        // the first commit upload should never carry that onto a
        // memory-limited host. The truncation marker stays in the record.
        error: `model: ${truncateText(msg, RECORDED_ERROR_BYTES)}`,
        retry_after_ms: retryable ? mErr?.retryAfterMs : undefined,
        events: retryable ? [] : events,
      });
      // Bound the log line too — a provider error can be megabytes.
      this.log("turn failed at model", {
        turn_id: turn.turn_id,
        round,
        attempt: turn.attempt,
        retryable,
        retry_after_ms: retryable ? mErr?.retryAfterMs : undefined,
        error: truncateText(msg, 4 * 1024),
      });
      return { failed: true, retryable };
    }
    try {
      const saved = await state.savePlan(personaId, gen, {
        turnId: turn.turn_id,
        round,
        // Display text and usage metadata are normalized: a stray NUL in a
        // reply must not void otherwise valid work. Tool call arguments are
        // NOT normalized — they are authoritative effect content, and the
        // store rejecting them is an honest recorded failure.
        text: stripNul(text),
        calls: calls.map((c) => ({
          call_id: c.id,
          tool: c.name,
          route: c.route,
          request: c.arguments,
        })),
        usage: stripNulDeep(usage) as Record<string, unknown>,
      });
      // Always execute the stored plan — a lost-response resend returns
      // the identical rounds; a conflict never silently substitutes.
      return { rounds: saved.plan.plan };
    } catch (e) {
      if (e instanceof FencedError) throw e;
      if (e instanceof StateError && e.status === 409) {
        // A different decision is already recorded at this round. Fail
        // loudly rather than executing off the wrong decision.
        const msg = stripNul(e instanceof Error ? e.message : String(e));
        await this.failPermanent(
          turn,
          events,
          `conflicting plan at round ${round}: ${msg}`,
        );
        return { failed: true, retryable: false };
      }
      if (e instanceof StateError && e.status === 400) {
        // The decision itself cannot be recorded (deterministic rejection —
        // e.g. a NUL jsonb cannot hold). Retrying savePlan or re-planning
        // can never succeed, so commit a recorded non-retryable failure
        // rather than leaving the input to poison the queue. (CR3-B1.)
        const msg = stripNul(e instanceof Error ? e.message : String(e));
        await this.failPermanent(
          turn,
          events,
          `decision could not be recorded: ${msg}`,
        );
        return { failed: true, retryable: false };
      }
      // Transient: no effect committed yet — leave the turn running for
      // recovery to retry the save (idempotent on identical body).
      throw e;
    }
  }

  /**
   * Commit a non-retryable failure: the input resolves done, the journal
   * keeps the events so far, the turn row records why, and the outbox
   * carries a turn_failed record so the requester is not left waiting
   * silently. Used for durable-plan boundary violations and for decisions
   * the store can never record — no fabricated tool result, no poison
   * requeue loop, no duplicate effect.
   */
  private async failPermanent(
    turn: Turn,
    events: { kind: string; payload: Record<string, unknown> }[],
    error: string,
  ): Promise<void> {
    await this.commitTurnFinal(turn, {
      outcome: "fail",
      retryable: false,
      error: stripNul(error),
      events,
    });
    this.log("turn failed permanently", {
      turn_id: turn.turn_id,
      error,
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
const TRUNC_MARK = "…[truncated]";

/** Replace NUL with U+FFFD recursively — jsonb can never hold 0x00. */
function scrubJson(v: unknown): unknown {
  if (typeof v === "string") return v.replaceAll("\u0000", "\uFFFD");
  if (Array.isArray(v)) return v.map(scrubJson);
  if (v !== null && typeof v === "object") {
    return Object.fromEntries(
      Object.entries(v).map(([k, x]) => [k, scrubJson(x)]),
    );
  }
  return v;
}

/**
 * Bound a string's UTF-8 encoding, cutting only at a code-point boundary
 * so multibyte and control characters can never push the result past
 * maxBytes. Truncation is explicit — the marker is part of the record.
 */
function truncateText(s: string, maxBytes: number): string {
  const enc = new TextEncoder().encode(s);
  if (enc.length <= maxBytes) return s;
  const markLen = new TextEncoder().encode(TRUNC_MARK).length;
  let end = Math.max(0, maxBytes - markLen);
  while (end > 0 && ((enc[end] ?? 0) & 0xc0) === 0x80) end--;
  return new TextDecoder().decode(enc.subarray(0, end)) + TRUNC_MARK;
}

const SYSTEM =
  "You are a personal secretary — one continuing life across restarts, not a stateless handler. " +
  "Your journal is your durable memory. You may schedule.set future wake-ups and journal.note what matters. " +
  "message.send speaks into the shared channel as you — it only runs as an elevated call, and waits for the human's explicit approval before it is sent; a normal call is blocked without asking anyone. " +
  "For any tool call, choose route 'normal' to act under your own authority, or 'elevated' to ask the human for a one-shot approval first; elevated never bypasses a denial. " +
  "When the user asks you to remember something, call journal.note before confirming — never claim a note you did not write. " +
  "After tool calls complete, their results are returned to you — then reply to the user, truthfully reflecting what actually happened. " +
  "Keep replies brief and honest; do not claim abilities you do not have.";

function inputReceivedEvent(input: Input, turn: Turn): EventInput {
  return {
    kind: "input_received",
    payload: {
      input_id: input.input_id,
      kind: input.kind,
      text: typeof input.payload.text === "string" ? input.payload.text : null,
      actor_kind: input.actor_kind,
      source_surface: input.source_surface,
      attempt: turn.attempt,
    },
  };
}

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

/** Strip bytes the durable store cannot persist (PG text/jsonb reject NUL). */
function stripNul(s: string): string {
  return s.replace(/\u0000/g, "");
}

function stripNulDeep(v: unknown): unknown {
  if (typeof v === "string") return stripNul(v);
  if (Array.isArray(v)) return v.map(stripNulDeep);
  if (v !== null && typeof v === "object") {
    return Object.fromEntries(
      Object.entries(v as Record<string, unknown>).map(([k, x]) => [
        stripNul(k),
        stripNulDeep(x),
      ]),
    );
  }
  return v;
}

/**
 * True for transport-level failures — fetch/DNS/socket errors and aborts
 * carry no HTTP status but are transient by nature. Distinguished from
 * other unknown exceptions so a deterministic defect on a live turn can
 * be bounded instead of retried forever.
 */
function isTransportError(e: unknown): boolean {
  if (!(e instanceof Error)) return true; // non-Error throw: cannot classify — treat as transient
  if (e.name === "AbortError") return true;
  return (
    e instanceof TypeError &&
    /fetch|network|terminated|socket|connect|ECONN|ENOTFOUND|EAI_AGAIN/i.test(
      e.message,
    )
  );
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(t);
      resolve();
    };
    const t = setTimeout(() => {
      // The abort listener must not outlive the timer — a long-running
      // loop would otherwise accumulate one closure per sleep on the
      // signal (fresh-review F4).
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
