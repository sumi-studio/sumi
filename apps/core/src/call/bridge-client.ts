import type {
  CallSession,
  CallTicket,
  CallUtterance,
  CallUtteranceStatus,
} from "../types.ts";

/**
 * Raised when the state service reports this runner's session claim is gone:
 * superseded epoch, another runner, expired lease, or a terminal session.
 * The runner's only correct response is to tear down media and stop — it
 * must never mint media or move state without the claim.
 */
export class CallClaimLostError extends Error {
  constructor(message = "call session claim lost") {
    super(message);
    this.name = "CallClaimLostError";
  }
}

export class CallBridgeError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "CallBridgeError";
    this.status = status;
  }
}

type FetchLike = (
  input: string,
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

/**
 * The media runner's window onto call-session authority: the persona-scoped
 * /calls routes. Claim authority — never writer generation — gates every
 * mutation, so a stale runner cannot mint tickets or move dispositions.
 */
export class CallBridgeClient {
  private readonly baseUrl: string;
  private readonly token: string;
  private readonly fetchImpl: FetchLike;

  constructor(
    baseUrl: string,
    token: string,
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
      return (await res.json()) as T;
    }
    let message = `call bridge ${res.status}`;
    try {
      const parsed = (await res.json()) as { error?: string };
      if (parsed.error) message = parsed.error;
    } catch {
      /* non-JSON error body */
    }
    if (res.status === 409) throw new CallClaimLostError(message);
    throw new CallBridgeError(res.status, message);
  }

  async claimSessions(
    persona: string,
    req: { runnerId: string; leaseMs: number; limit?: number },
  ): Promise<CallSession[]> {
    const res = await this.call<{ sessions: CallSession[] }>(
      "POST",
      `/internal/core/personas/${persona}/calls/claim`,
      {
        runner_id: req.runnerId,
        lease_ms: req.leaseMs,
        limit: req.limit ?? 4,
      },
    );
    return res.sessions;
  }

  async heartbeatSession(
    persona: string,
    sessionId: string,
    req: { runnerId: string; epoch: number; leaseMs: number },
  ): Promise<CallSession> {
    const res = await this.call<{ session: CallSession }>(
      "POST",
      `/internal/core/personas/${persona}/calls/sessions/${encodeURIComponent(sessionId)}/heartbeat`,
      { runner_id: req.runnerId, epoch: req.epoch, lease_ms: req.leaseMs },
    );
    return res.session;
  }

  async sessionTicket(
    persona: string,
    sessionId: string,
    req: { runnerId: string; epoch: number },
  ): Promise<CallTicket> {
    const res = await this.call<{ ticket: CallTicket }>(
      "POST",
      `/internal/core/personas/${persona}/calls/sessions/${encodeURIComponent(sessionId)}/ticket`,
      { runner_id: req.runnerId, epoch: req.epoch },
    );
    return res.ticket;
  }

  async reportSessionStatus(
    persona: string,
    sessionId: string,
    req: {
      runnerId: string;
      epoch: number;
      status: "active" | "ending" | "ended" | "failed";
      reason?: string;
    },
  ): Promise<CallSession> {
    const res = await this.call<{ session: CallSession }>(
      "POST",
      `/internal/core/personas/${persona}/calls/sessions/${encodeURIComponent(sessionId)}/status`,
      {
        runner_id: req.runnerId,
        epoch: req.epoch,
        status: req.status,
        reason: req.reason ?? "",
      },
    );
    return res.session;
  }

  async pendingUtterances(
    persona: string,
    sessionId: string,
    req: { runnerId: string; epoch: number },
  ): Promise<CallUtterance[]> {
    const res = await this.call<{ utterances: CallUtterance[] }>(
      "GET",
      `/internal/core/personas/${persona}/calls/sessions/${encodeURIComponent(sessionId)}/utterances?runner_id=${encodeURIComponent(req.runnerId)}&epoch=${req.epoch}`,
    );
    return res.utterances;
  }

  /**
   * Deliver one transcript into the secretary's durable input stream. Lives
   * here because it rides the same persona-token auth; it is the ordinary
   * inputs route, not a bridge-authority route.
   */
  async submitInput(
    persona: string,
    input: {
      inputId: string;
      kind: string;
      attention: "reply" | "observe" | "defer";
      actorKind: string;
      actorId: string;
      sourceSurface: string;
      threadId: string;
      occurredAt?: string;
      payload: Record<string, unknown>;
    },
  ): Promise<void> {
    await this.call("POST", `/internal/core/personas/${persona}/inputs`, {
      input_id: input.inputId,
      kind: input.kind,
      attention: input.attention,
      actor_kind: input.actorKind,
      actor_id: input.actorId,
      source_surface: input.sourceSurface,
      thread_id: input.threadId,
      occurred_at: input.occurredAt,
      payload: input.payload,
    });
  }

  async reportUtterance(
    persona: string,
    sessionId: string,
    utteranceId: string,
    req: {
      runnerId: string;
      epoch: number;
      status: CallUtteranceStatus;
      detail?: Record<string, unknown>;
    },
  ): Promise<CallUtterance> {
    const res = await this.call<{ utterance: CallUtterance }>(
      "POST",
      `/internal/core/personas/${persona}/calls/sessions/${encodeURIComponent(sessionId)}/utterances/${encodeURIComponent(utteranceId)}/disposition`,
      {
        runner_id: req.runnerId,
        epoch: req.epoch,
        status: req.status,
        detail: req.detail ?? {},
      },
    );
    return res.utterance;
  }
}
