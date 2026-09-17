import type {
  Approval,
  ApprovalDecision,
  ClaimedMemoryChunk,
  CommitRequest,
  Event,
  FundingRef,
  Job,
  JobTerminalReport,
  LoadResult,
  MemoryChunk,
  MemoryStatus,
  ModelBinding,
  Operation,
  OutboxEntry,
  PersonaState,
  PlanCall,
  RecoverResult,
  Schedule,
  Turn,
  TurnPlan,
  UsageAdmitResult,
  UsageEstimate,
  UsageFact,
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
  /**
   * Reserve priced-estimate spend for one provider call, under the
   * writer's generation, before any request bytes are sent. Replaying the
   * same admit (lost response) returns the held reservation rather than
   * double-reserving. A denial creates nothing durable — the caller
   * commits the wait itself at turn commit or reshelves the chunk.
   */
  admitUsage(
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
  ): Promise<UsageAdmitResult>;
  /**
   * Record one call's resolved usage. Deliberately not writer-fenced: the
   * spend already happened and a replaced writer must still be able to
   * report it. Idempotent on fact_id — an identical redelivery replays
   * the stored fact; a conflicting payload under a known fact_id is a
   * 409 contract violation (a genuinely additional call must carry a
   * fresh fact_id).
   */
  recordUsage(
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
  ): Promise<{ fact: UsageFact; created: boolean }>;
  /** The persona's usage ledger, oldest first. */
  listUsageFacts(persona: string, limit?: number): Promise<UsageFact[]>;
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
  /**
   * Memory housekeeping between turns: seal newly safe journal ranges,
   * then apply shelved L1 replacements while the live raw estimate
   * exceeds the limit. Runs under the writer generation so application
   * never interleaves with an in-flight model call.
   */
  memoryMaintain(persona: string, generation: number): Promise<MemoryStatus>;
  memoryStatus(persona: string): Promise<MemoryStatus>;
  /**
   * Claim the oldest sealable chunk for asynchronous L1 preparation — one
   * branch at a time. `chunk` is null when nothing is claimable right now
   * (nothing sealed, another branch preparing, or backoff pending). The
   * returned context is the rendered parent context at claim time.
   */
  claimMemoryChunk(
    persona: string,
    generation: number,
    contextLimit: number,
  ): Promise<ClaimedMemoryChunk>;
  /**
   * Shelve the finished replacement candidate. Completion alone never
   * changes the sent context — application is a separate threshold-gated
   * step in memoryMaintain. keepUnchanged is the model's KEEP_UNCHANGED
   * decision: the originals stay and the chunk is never reprepared.
   */
  completeMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    result: { replacement?: string; keepUnchanged?: boolean },
  ): Promise<MemoryChunk>;
  /**
   * Record a failed preparation attempt. Retryable failures return the
   * chunk to the shelf with backoff while attempts remain; an exhausted
   * or non-retryable failure is terminal ('failed') — visible, originals
   * kept, never silently skipped.
   */
  failMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    failure: { error: string; retryable: boolean },
  ): Promise<MemoryChunk>;
  /**
   * Return a claimed chunk to the shelf when no model request could be
   * evaluated — an unbound selection, a missing credential, a
   * binding-lookup outage, or a denied budget admission. Records no
   * verdict and spends no attempts or interruptions; the chunk waits out
   * a short pacing (delayMs, or the service default), then proceeds once
   * a usable binding exists. A funding or model-selection change clears
   * budget pacing early.
   */
  reshelveMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    pause: { reason: string; delayMs?: number },
  ): Promise<MemoryChunk>;
  outbox(
    persona: string,
    afterSeq: number,
    limit?: number,
  ): Promise<OutboxEntry[]>;
  /**
   * The tools this store can actually execute for the persona — internal
   * tools plus host-registered delegated effects (e.g. messaging.send only
   * when Messaging is wired). The core offers the model exactly this set.
   */
  listTools(persona: string): Promise<string[]>;
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

type StateResponse = {
  ok: boolean;
  status: number;
  json(): Promise<unknown>;
  text(): Promise<string>;
};

type FetchLike = (
  input: string | URL,
  init?: {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
    signal?: AbortSignal;
  },
) => Promise<StateResponse>;

/**
 * Per-call deadline for a state request, covering both the response
 * headers and the body read: the same AbortSignal governs fetch() and its
 * pending json() read. 10s is far above a healthy call (small JSON over a
 * LAN/binding hop, tens of ms) and matches the Go wake client's timeout —
 * a call that has not finished by then is a wedged transport, and waiting
 * longer only stalls everyone queued behind it on startSerialized. A
 * timed-out call is indeterminate (the service may have committed); it is
 * classified transient so the caller's normal retry path replays it under
 * the server-side idempotency and generation fencing that already cover
 * uncertain outcomes.
 */
export const STATE_CALL_TIMEOUT_MS = 10_000;

/**
 * The deadline's rejection shape. call() passes only its own timeout
 * signal, so an abort here is always the deadline expiring — reported as a
 * transient 503 rather than leaking an undifferentiated AbortError, which
 * some callers would classify as a non-transient unknown defect.
 */
function callDeadlineError(
  e: unknown,
  method: string,
  path: string,
  timeoutMs: number,
): StateError | null {
  if (
    e instanceof Error &&
    (e.name === "TimeoutError" || e.name === "AbortError")
  )
    return new StateError(
      503,
      `state ${method} ${path} timed out after ${timeoutMs}ms`,
    );
  return null;
}

/** HTTP client for the Go agentstate service. */
export class HttpStateClient implements StateClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: FetchLike;
  private readonly timeoutMs: number;

  constructor(
    baseUrl: string,
    token: string,
    // Wrap rather than extract `fetch`: workerd throws Illegal Invocation on
    // a detached reference to the global function.
    fetchImpl: FetchLike = (input, init) =>
      fetch(input, init) as ReturnType<FetchLike>,
    timeoutMs = STATE_CALL_TIMEOUT_MS,
  ) {
    this.baseUrl = baseUrl;
    this.token = token;
    this.fetchImpl = fetchImpl;
    this.timeoutMs = timeoutMs;
  }

  private async call<T>(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<T> {
    // One signal bounds the whole request: a service that accepts but never
    // answers, or answers headers and stalls mid-body, fails here instead
    // of occupying a serialized start (or a drain) forever.
    // An owned controller — not AbortSignal.timeout — because the runtime's
    // timeout signal leaves a pending timer until the deadline even after
    // the exchange completes, which keeps a Durable Object
    // non-hibernateable (and billed) for the remainder of the window.
    // clearTimeout on every exit releases the deadline once the body has
    // settled; a still-pending call keeps its bound.
    const deadline = new AbortController();
    const timer = setTimeout(() => deadline.abort(), this.timeoutMs);
    try {
      let res: StateResponse;
      try {
        res = await this.fetchImpl(this.baseUrl + path, {
          method,
          headers: {
            Authorization: `Bearer ${this.token}`,
            "Content-Type": "application/json",
          },
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: deadline.signal,
        });
      } catch (e) {
        throw callDeadlineError(e, method, path, this.timeoutMs) ?? e;
      }
      if (res.ok) {
        try {
          return (await res.json()) as T;
        } catch (e) {
          const timeout = callDeadlineError(e, method, path, this.timeoutMs);
          if (timeout) throw timeout;
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
    } finally {
      clearTimeout(timer);
    }
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
  admitUsage(
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
  ) {
    return this.call<UsageAdmitResult>(
      "POST",
      `/internal/core/personas/${persona}/usage/admit`,
      {
        generation,
        fact_id: req.factId,
        kind: req.kind,
        phase: req.phase,
        turn_id: req.turnId,
        input_id: req.inputId,
        round: req.round ?? 0,
        funding: req.funding,
        estimate: req.estimate,
      },
    );
  }
  recordUsage(
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
  ) {
    return this.call<{ fact: UsageFact; created: boolean }>(
      "POST",
      `/internal/core/personas/${persona}/usage/record`,
      {
        fact_id: req.factId,
        kind: req.kind,
        phase: req.phase,
        turn_id: req.turnId,
        input_id: req.inputId,
        round: req.round ?? 0,
        funding: req.funding,
        status: req.status,
        input_tokens: req.inputTokens ?? null,
        output_tokens: req.outputTokens ?? null,
        cached_tokens: req.cachedTokens ?? null,
        quantities: req.quantities ?? {},
      },
    );
  }
  async listUsageFacts(persona: string, limit = 100) {
    const res = await this.call<{ facts: UsageFact[] }>(
      "GET",
      `/internal/core/personas/${persona}/usage/facts?limit=${limit}`,
    );
    return res.facts;
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
    }>("POST", `/internal/core/personas/${persona}/operations/claim`, {
      generation,
      operation_id: op.operationId,
      turn_id: op.turnId,
      tool: op.tool,
      call_index: op.callIndex,
      request: op.request,
    });
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
  memoryMaintain(persona: string, generation: number) {
    return this.call<MemoryStatus>(
      "POST",
      `/internal/core/personas/${persona}/memory/maintain`,
      { generation },
    );
  }
  memoryStatus(persona: string) {
    return this.call<MemoryStatus>(
      "GET",
      `/internal/core/personas/${persona}/memory`,
    );
  }
  claimMemoryChunk(persona: string, generation: number, contextLimit: number) {
    return this.call<ClaimedMemoryChunk>(
      "POST",
      `/internal/core/personas/${persona}/memory/chunks/claim`,
      { generation, context_limit: contextLimit },
    );
  }
  async completeMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    result: { replacement?: string; keepUnchanged?: boolean },
  ) {
    const res = await this.call<{ chunk: MemoryChunk }>(
      "POST",
      `/internal/core/personas/${persona}/memory/chunks/${chunkSeq}/complete`,
      {
        generation,
        replacement: result.replacement ?? "",
        keep_unchanged: result.keepUnchanged ?? false,
      },
    );
    return res.chunk;
  }
  async failMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    failure: { error: string; retryable: boolean },
  ) {
    const res = await this.call<{ chunk: MemoryChunk }>(
      "POST",
      `/internal/core/personas/${persona}/memory/chunks/${chunkSeq}/fail`,
      { generation, error: failure.error, retryable: failure.retryable },
    );
    return res.chunk;
  }
  async reshelveMemoryChunk(
    persona: string,
    generation: number,
    chunkSeq: number,
    pause: { reason: string; delayMs?: number },
  ) {
    const res = await this.call<{ chunk: MemoryChunk }>(
      "POST",
      `/internal/core/personas/${persona}/memory/chunks/${chunkSeq}/reshelve`,
      { generation, reason: pause.reason, delay_ms: pause.delayMs },
    );
    return res.chunk;
  }
  async outbox(persona: string, afterSeq: number, limit = 200) {
    const res = await this.call<{ outbox: OutboxEntry[] }>(
      "GET",
      `/internal/core/personas/${persona}/outbox?after_seq=${afterSeq}&limit=${limit}`,
    );
    return res.outbox;
  }
  async listTools(persona: string) {
    const res = await this.call<{ tools: string[] }>(
      "GET",
      `/internal/core/personas/${persona}/tools`,
    );
    return res.tools;
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
