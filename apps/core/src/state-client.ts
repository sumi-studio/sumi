import type {
  CommitRequest,
  Event,
  Job,
  JobTerminalReport,
  LoadResult,
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
  /**
   * Present when a 409 job response carries the stored row — e.g. a
   * heartbeat answered after the job went terminal or the claim expired.
   * The runner reads job.status to decide what to do with the execution
   * it still holds.
   */
  readonly job?: Job;
  constructor(status: number, message: string, job?: Job) {
    super(message);
    this.name = "StateError";
    this.status = status;
    this.job = job;
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
   * Persist the model's decision for the claimed input before any of its
   * effects run. Idempotent: an identical resend returns the stored plan
   * with `created: false`; a conflicting plan is rejected 409.
   */
  savePlan(
    persona: string,
    generation: number,
    req: {
      turnId: string;
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
   * claimed (tool, request) equals plan.calls[call_index] — a caller never
   * supplies an idempotency key.
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
  /**
   * Record a background job for later runner claiming. Idempotent on
   * job_id: an identical resend returns `created: false` with the stored
   * row; a divergent one is rejected 409.
   */
  submitJob(
    persona: string,
    job: { jobId: string; kind: string; request: Record<string, unknown> },
  ): Promise<{ job: Job; created: boolean }>;
  getJob(persona: string, jobId: string): Promise<Job>;
  listJobs(
    persona: string,
    opts?: { status?: Job["status"][]; limit?: number },
  ): Promise<Job[]>;
  /**
   * queued → cancelled (notification queued); running → cancel_requested
   * for the owning runner to observe via heartbeat. Not writer-gated:
   * callable while the secretary is down.
   */
  cancelJob(persona: string, jobId: string): Promise<Job>;
  /**
   * The runner's periodic call: sweeps expired claims to 'lost' (with
   * notification — indeterminate, never re-run) and claims up to `limit`
   * queued jobs of the requested kinds for this runner.
   */
  claimJobs(
    persona: string,
    req: {
      runnerId: string;
      kinds: string[];
      leaseMs: number;
      limit?: number;
    },
  ): Promise<{ claimed: Job[]; swept: Job[] }>;
  /**
   * Extend the runner's claim and learn the current status (incl.
   * cancel_requested). Throws StateError(409) with `job` set when the job
   * is no longer this runner's — terminal, lost, or claimed away.
   */
  heartbeatJob(
    persona: string,
    jobId: string,
    req: { runnerId: string; leaseMs: number },
  ): Promise<Job>;
  /**
   * Record the runner-observed terminal outcome and queue the secretary's
   * notification atomically. An identical resend returns the stored row
   * (lost response); a divergent one throws StateError(409).
   */
  completeJob(
    persona: string,
    jobId: string,
    req: {
      runnerId: string;
      status: JobTerminalReport;
      result: Record<string, unknown>;
      error?: string;
    },
  ): Promise<Job>;
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
    let job: Job | undefined;
    try {
      const parsed = (await res.json()) as { error?: string; job?: Job };
      if (parsed.error) message = parsed.error;
      if (parsed.job) job = parsed.job;
    } catch {
      /* non-JSON error body */
    }
    if (res.status === 401) throw new UnauthorizedError(message);
    if (res.status === 409 && message.includes("fenced"))
      throw new FencedError(message);
    throw new StateError(res.status, message, job);
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
    return this.call<{ operation: Operation; fresh: boolean }>(
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
  async submitJob(
    persona: string,
    job: { jobId: string; kind: string; request: Record<string, unknown> },
  ) {
    return this.call<{ job: Job; created: boolean }>(
      "POST",
      `/internal/core/personas/${persona}/jobs`,
      { job_id: job.jobId, kind: job.kind, request: job.request },
    );
  }
  async getJob(persona: string, jobId: string) {
    const res = await this.call<{ job: Job }>(
      "GET",
      `/internal/core/personas/${persona}/jobs/${encodeURIComponent(jobId)}`,
    );
    return res.job;
  }
  async listJobs(
    persona: string,
    opts?: { status?: Job["status"][]; limit?: number },
  ) {
    const params = new URLSearchParams();
    if (opts?.status?.length) params.set("status", opts.status.join(","));
    if (opts?.limit) params.set("limit", String(opts.limit));
    const qs = params.toString();
    const res = await this.call<{ jobs: Job[] }>(
      "GET",
      `/internal/core/personas/${persona}/jobs${qs ? `?${qs}` : ""}`,
    );
    return res.jobs;
  }
  async cancelJob(persona: string, jobId: string) {
    const res = await this.call<{ job: Job }>(
      "POST",
      `/internal/core/personas/${persona}/jobs/${encodeURIComponent(jobId)}/cancel`,
      {},
    );
    return res.job;
  }
  async claimJobs(
    persona: string,
    req: { runnerId: string; kinds: string[]; leaseMs: number; limit?: number },
  ) {
    return this.call<{ claimed: Job[]; swept: Job[] }>(
      "POST",
      `/internal/core/personas/${persona}/jobs/claim`,
      {
        runner_id: req.runnerId,
        kinds: req.kinds,
        lease_ms: req.leaseMs,
        limit: req.limit,
      },
    );
  }
  async heartbeatJob(
    persona: string,
    jobId: string,
    req: { runnerId: string; leaseMs: number },
  ) {
    const res = await this.call<{ job: Job }>(
      "POST",
      `/internal/core/personas/${persona}/jobs/${encodeURIComponent(jobId)}/heartbeat`,
      { runner_id: req.runnerId, lease_ms: req.leaseMs },
    );
    return res.job;
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
  ) {
    const res = await this.call<{ job: Job }>(
      "POST",
      `/internal/core/personas/${persona}/jobs/${encodeURIComponent(jobId)}/complete`,
      {
        runner_id: req.runnerId,
        status: req.status,
        result: req.result,
        error: req.error,
      },
    );
    return res.job;
  }
}
