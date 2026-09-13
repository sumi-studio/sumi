import type {
  Approval,
  ApprovalDecision,
  CommitRequest,
  Event,
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

/** Raised when the state service reports our writer generation is fenced. */
export class FencedError extends Error {
  constructor(message = "writer generation fenced") {
    super(message);
    this.name = "FencedError";
  }
}

/** Raised on 401 — the persona capability token is missing or wrong. */
export class UnauthorizedError extends Error {
  constructor(message = "unauthorized") {
    super(message);
    this.name = "UnauthorizedError";
  }
}

export class StateError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "StateError";
    this.status = status;
  }
}

/**
 * The core's only window onto canonical state. Every mutating call carries
 * the writer generation; the service enforces the fence transactionally.
 * Implementations: HttpStateClient (real) and the in-memory fake in tests.
 */
export interface StateClient {
  acquireWriter(
    persona: string,
    holder: string,
    ttlMs: number,
  ): Promise<WriterLease>;
  renewWriter(
    persona: string,
    holder: string,
    generation: number,
    ttlMs: number,
  ): Promise<WriterLease>;
  releaseWriter(
    persona: string,
    holder: string,
    generation: number,
  ): Promise<void>;
  recover(persona: string, generation: number): Promise<RecoverResult>;
  loadTurn(
    persona: string,
    generation: number,
    turnId: string,
    contextLimit: number,
  ): Promise<LoadResult>;
  /**
   * Persist one round of the model's decisions for the claimed input before
   * any of that round's effects run. The plan is an append-only list of
   * rounds: an identical resend of a recorded round returns the stored plan
   * with `created: false`; a different decision at a recorded position or a
   * skipped round is rejected 409.
   */
  savePlan(
    persona: string,
    generation: number,
    req: {
      turnId: string;
      round: number;
      text: string;
      calls: PlanCall[];
      usage: Record<string, unknown>;
    },
  ): Promise<{ plan: TurnPlan; created: boolean }>;
  commitTurn(
    persona: string,
    turnId: string,
    generation: number,
    req: CommitRequest,
  ): Promise<Turn>;
  events(persona: string, afterSeq: number, limit?: number): Promise<Event[]>;
  /**
   * Claim one position of the recorded plan. The service derives the durable
   * effect identity server-side (input_id + call_index) and verifies the
   * claimed (tool, request) equals the recorded call at that flat position
   * across all plan rounds — a caller never supplies an idempotency key.
   *
   * A gated call (intrinsic tool requirement or elevated route, ADR 0013)
   * parks instead of running: the returned operation is
   * "awaiting_approval" and `approval` carries the durable pending record.
   * After the human approves, the next claim of the same position executes
   * exactly once; after denial the claim replays the durable failure.
   */
  claimOperation(
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
  }>;
  /**
   * Pending tool approvals for the persona — the human decision surface.
   * Passing `approvalId` fetches one record.
   */
  listApprovals(persona: string, approvalId?: string): Promise<Approval[]>;
  /**
   * The authenticated human's one-shot decision on an approval. Identical
   * replays (same decision_id) return the stored record; a different
   * decision after the fact is rejected.
   */
  resolveApproval(
    persona: string,
    approvalId: string,
    decision: ApprovalDecision,
  ): Promise<Approval>;
  /**
   * The persona's resolved model binding — the user-selected connection's
   * identity and model. Authoritative: the core must use exactly this
   * binding or report it unavailable; never substitute another provider.
   */
  modelBinding(persona: string): Promise<ModelBinding>;
  completeOperation(
    persona: string,
    operationId: string,
    generation: number,
    response: Record<string, unknown>,
    failed: boolean,
  ): Promise<Operation>;
  dispatchSchedules(
    persona: string,
    generation: number,
    now?: Date,
    limit?: number,
  ): Promise<Schedule[]>;
  outbox(
    persona: string,
    afterSeq: number,
    limit?: number,
  ): Promise<OutboxEntry[]>;
  personaState(persona: string): Promise<PersonaState>;
}

type FetchLike = (
  input: string | URL,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
    signal?: AbortSignal;
  },
) => Promise<{
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
  text(): Promise<string>;
}>;

/** HTTP client for the Go agentstate service. */
export class HttpStateClient implements StateClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: FetchLike;

  constructor(
    baseUrl: string,
    token: string,
    // Wrap rather than extract `fetch`: workerd throws Illegal Invocation on
    // a detached reference to the global function.
    fetchImpl: FetchLike = (input, init) =>
      fetch(input, init) as ReturnType<FetchLike>,
  ) {
    this.baseUrl = baseUrl;
    this.token = token;
    this.fetchImpl = fetchImpl;
  }

  private async call<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    const res = await this.fetchImpl(this.baseUrl + path, {
      method,
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (res.ok) {
      try {
        return (await res.json()) as T;
      } catch (e) {
        // A 200 with an unreadable body is an infrastructure blip — a
        // truncated proxy/middlebox response or a service bug — not a
        // code defect. Surface it as a transient 5xx so callers back
        // off instead of exiting (final-review NF2). The real status
        // stays in the message.
        throw new StateError(
          503,
          `state service returned an unreadable ${res.status} body: ${e instanceof Error ? e.message : String(e)}`,
        );
      }
    }
    let message = `state service ${res.status}`;
    try {
      const parsed = (await res.json()) as { error?: string };
      if (parsed.error) message = parsed.error;
    } catch {
      /* non-JSON error body */
    }
    if (res.status === 401) throw new UnauthorizedError(message);
    if (res.status === 409 && message.includes("fenced"))
      throw new FencedError(message);
    throw new StateError(res.status, message);
  }

  acquireWriter(persona: string, holder: string, ttlMs: number) {
    return this.call<WriterLease>(
      "POST",
      `/internal/core/personas/${persona}/writer/acquire`,
      {
        holder_id: holder,
        ttl_ms: ttlMs,
      },
    );
  }
  renewWriter(
    persona: string,
    holder: string,
    generation: number,
    ttlMs: number,
  ) {
    return this.call<WriterLease>(
      "POST",
      `/internal/core/personas/${persona}/writer/renew`,
      {
        holder_id: holder,
        generation,
        ttl_ms: ttlMs,
      },
    );
  }
  async releaseWriter(persona: string, holder: string, generation: number) {
    await this.call(
      "POST",
      `/internal/core/personas/${persona}/writer/release`,
      {
        holder_id: holder,
        generation,
      },
    );
  }
  recover(persona: string, generation: number) {
    return this.call<RecoverResult>(
      "POST",
      `/internal/core/personas/${persona}/recover`,
      {
        generation,
      },
    );
  }
  loadTurn(
    persona: string,
    generation: number,
    turnId: string,
    contextLimit: number,
  ) {
    return this.call<LoadResult>(
      "POST",
      `/internal/core/personas/${persona}/turns/load`,
      {
        generation,
        turn_id: turnId,
        context_limit: contextLimit,
      },
    );
  }
  savePlan(
    persona: string,
    generation: number,
    req: {
      turnId: string;
      round: number;
      text: string;
      calls: PlanCall[];
      usage: Record<string, unknown>;
    },
  ) {
    return this.call<{ plan: TurnPlan; created: boolean }>(
      "POST",
      `/internal/core/personas/${persona}/turns/plan`,
      {
        generation,
        turn_id: req.turnId,
        round: req.round,
        text: req.text,
        calls: req.calls,
        usage: req.usage,
      },
    );
  }
  async commitTurn(
    persona: string,
    turnId: string,
    generation: number,
    req: CommitRequest,
  ) {
    const res = await this.call<{ turn: Turn }>(
      "POST",
      `/internal/core/personas/${persona}/turns/${turnId}/commit`,
      { generation, ...req },
    );
    return res.turn;
  }
  async events(persona: string, afterSeq: number, limit = 200) {
    const res = await this.call<{ events: Event[] }>(
      "GET",
      `/internal/core/personas/${persona}/events?after_seq=${afterSeq}&limit=${limit}`,
    );
    return res.events;
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
  ) {
    // No client idempotency_key: effect identity is server-derived
    // (input_id + call_index) so a caller can never choose a fresh key
    // for an already-decided position.
    return this.call<{
      operation: Operation;
      approval: Approval | null;
      fresh: boolean;
    }>(
      "POST",
      `/internal/core/personas/${persona}/operations/claim`,
      {
        generation,
        operation_id: op.operationId,
        turn_id: op.turnId,
        tool: op.tool,
        call_index: op.callIndex,
        request: op.request,
      },
    );
  }
  async listApprovals(persona: string, approvalId?: string) {
    if (approvalId) {
      const one = await this.call<{ approval: Approval }>(
        "GET",
        `/internal/core/personas/${persona}/approvals/${encodeURIComponent(approvalId)}`,
      );
      return [one.approval];
    }
    const res = await this.call<{ approvals: Approval[] }>(
      "GET",
      `/internal/core/personas/${persona}/approvals`,
    );
    return res.approvals;
  }
  async resolveApproval(
    persona: string,
    approvalId: string,
    decision: ApprovalDecision,
  ) {
    const res = await this.call<{ approval: Approval }>(
      "POST",
      `/internal/core/personas/${persona}/approvals/${approvalId}/decision`,
      decision as unknown as Record<string, unknown>,
    );
    return res.approval;
  }
  modelBinding(persona: string) {
    return this.call<ModelBinding>(
      "GET",
      `/internal/core/personas/${persona}/model`,
    );
  }
  async completeOperation(
    persona: string,
    operationId: string,
    generation: number,
    response: Record<string, unknown>,
    failed: boolean,
  ) {
    const res = await this.call<{ operation: Operation }>(
      "POST",
      `/internal/core/personas/${persona}/operations/${operationId}/complete`,
      { generation, response, failed },
    );
    return res.operation;
  }
  async dispatchSchedules(
    persona: string,
    generation: number,
    now?: Date,
    limit = 16,
  ) {
    const res = await this.call<{ fired: Schedule[] }>(
      "POST",
      `/internal/core/personas/${persona}/schedules/dispatch`,
      { generation, now: now?.toISOString(), limit },
    );
    return res.fired;
  }
  async outbox(persona: string, afterSeq: number, limit = 200) {
    const res = await this.call<{ outbox: OutboxEntry[] }>(
      "GET",
      `/internal/core/personas/${persona}/outbox?after_seq=${afterSeq}&limit=${limit}`,
    );
    return res.outbox;
  }
  personaState(persona: string) {
    return this.call<PersonaState>(
      "GET",
      `/internal/core/personas/${persona}/state`,
    );
  }
}
