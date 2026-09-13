import type {
  CommitRequest,
  Event,
  LoadResult,
  Operation,
  OutboxEntry,
  PersonaState,
  RecoverResult,
  Schedule,
  Turn,
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
  commitTurn(
    persona: string,
    turnId: string,
    generation: number,
    req: CommitRequest,
  ): Promise<Turn>;
  events(persona: string, afterSeq: number, limit?: number): Promise<Event[]>;
  claimOperation(
    persona: string,
    generation: number,
    op: {
      operationId: string;
      turnId: string;
      tool: string;
      idempotencyKey: string;
      request: Record<string, unknown>;
    },
  ): Promise<{ operation: Operation; fresh: boolean }>;
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
    if (res.ok) return (await res.json()) as T;
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
      idempotencyKey: string;
      request: Record<string, unknown>;
    },
  ) {
    return this.call<{ operation: Operation; fresh: boolean }>(
      "POST",
      `/internal/core/personas/${persona}/operations/claim`,
      {
        generation,
        operation_id: op.operationId,
        turn_id: op.turnId,
        tool: op.tool,
        idempotency_key: op.idempotencyKey,
        request: op.request,
      },
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
