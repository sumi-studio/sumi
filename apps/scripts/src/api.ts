/**
 * Thin typed client for the state service job/file/usage routes the
 * runner owns. Auth is the internal runtime credential (or admin secret
 * in dev) — never a persona token and never anything readable by a
 * script. Every mutating call carries runner_id so the server can tie
 * the request to the recorded claim.
 */

export interface JobRow {
  job_id: string;
  persona_id: string;
  kind: string;
  status: string;
  request: unknown;
  result: Record<string, unknown> | null;
  claimed_by: string | null;
  claim_expires_at: string | null;
  error: string | null;
}

export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;
  constructor(status: number, body: unknown, message: string) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

export interface ClientOptions {
  api: string;
  token: string;
  timeoutMs?: number;
}

export class StateClient {
  readonly opts: ClientOptions;
  constructor(opts: ClientOptions) { this.opts = opts; }

  private async call(method: string, path: string, body?: unknown): Promise<unknown> {
    const res = await fetch(`${this.opts.api}${path}`, {
      method,
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${this.opts.token}`,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(this.opts.timeoutMs ?? 15_000),
    });
    const payload = await res.json().catch(() => null);
    if (!res.ok) {
      const msg = payload && typeof payload === "object" && "error" in payload
        ? String((payload as Record<string, unknown>).error)
        : `${method} ${path} -> ${res.status}`;
      throw new ApiError(res.status, payload, msg);
    }
    return payload;
  }

  claimJobs(personaID: string, runnerID: string, leaseMs: number, limit: number): Promise<{ claimed: JobRow[]; swept: JobRow[] }> {
    return this.call("POST", `/internal/core/personas/${personaID}/jobs/claim`, {
      runner_id: runnerID, kinds: ["script"], lease_ms: leaseMs, limit,
    }) as Promise<{ claimed: JobRow[]; swept: JobRow[] }>;
  }

  heartbeat(personaID: string, jobID: string, runnerID: string, leaseMs: number): Promise<{ job: JobRow }> {
    return this.call("POST", `/internal/core/personas/${personaID}/jobs/${jobID}/heartbeat`, {
      runner_id: runnerID, lease_ms: leaseMs,
    }) as Promise<{ job: JobRow }>;
  }

  complete(personaID: string, jobID: string, runnerID: string, status: string, result: Record<string, unknown>, error = ""): Promise<{ job: JobRow }> {
    return this.call("POST", `/internal/core/personas/${personaID}/jobs/${jobID}/complete`, {
      runner_id: runnerID, status, result, error,
    }) as Promise<{ job: JobRow }>;
  }

  getJob(personaID: string, jobID: string): Promise<{ job: JobRow }> {
    return this.call("GET", `/internal/core/personas/${personaID}/jobs/${jobID}`) as Promise<{ job: JobRow }>;
  }

  listFileOps(personaID: string, jobID: string, pendingOnly = false): Promise<{ ops: FileOpRow[]; pending: number }> {
    const q = pendingOnly ? "?pending=true" : "";
    return this.call("GET", `/internal/core/personas/${personaID}/jobs/${jobID}/files/ops${q}`) as Promise<{ ops: FileOpRow[]; pending: number }>;
  }

  resolveFileOp(personaID: string, jobID: string, opID: string, runnerID: string): Promise<{ op: FileOpRow }> {
    return this.call("POST", `/internal/core/personas/${personaID}/jobs/${jobID}/files/ops/${opID}/resolve`, {
      runner_id: runnerID,
    }) as Promise<{ op: FileOpRow }>;
  }

  recordUsage(personaID: string, fact: Record<string, unknown>): Promise<unknown> {
    return this.call("POST", `/internal/core/personas/${personaID}/usage/record`, fact);
  }

  /**
   * Proposed shared seam: attach this runner's observed outcome to a job
   * whose claim expired and was swept to 'lost'. 'lost' is an immutable
   * verdict — CompleteJob conflicts on it — so the observed result/usage
   * evidence attaches through a dedicated route that the shared job
   * owner (copy-lotus) is implementing. Consumer contract:
   *
   *   POST .../jobs/{j}/lost-outcome
   *   { runner_id, observed_status, result, error }
   *
   *   - 404 job: persona/job unknown.
   *   - 403: caller is not the job's original claiming runner.
   *   - 409: job is not 'lost' (live or already terminal — a real
   *     CompleteJob is the path for live claims).
   *   - 200: outcome stored WITHOUT changing the 'lost' verdict —
   *     e.g. result.lost_outcome = {...}; idempotent replay of an
   *     identical outcome; a divergent attach conflicts.
   *   - 404/501 route: seam not yet wired — caller keeps the evidence
   *     in its journal + usage facts, which ARE durable today.
   *
   * Returns the raw status so the caller can distinguish "not wired"
   * from a real refusal — this client does not invent a verdict.
   */
  async attachLostOutcome(personaID: string, jobID: string, runnerID: string, outcome: Record<string, unknown>): Promise<{ status: number; body: unknown }> {
    const res = await fetch(`${this.opts.api}/internal/core/personas/${personaID}/jobs/${jobID}/lost-outcome`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${this.opts.token}`,
      },
      body: JSON.stringify({ runner_id: runnerID, ...outcome }),
      signal: AbortSignal.timeout(this.opts.timeoutMs ?? 15_000),
    });
    return { status: res.status, body: await res.json().catch(() => null) };
  }
}

export interface FileOpRow {
  op_id: string;
  op_seq: number;
  op: string;
  status: string;
  path: string;
  result?: unknown;
  error: string | null;
}

/** Adapter for "which personas may have runnable script jobs" — the
 *  shared discovery seam (PersonasWithRunnableJobs) belongs to the
 *  sibling implementation; until root wires that route, the runner is
 *  configured with an explicit persona list. */
export interface Discovery {
  personas(): Promise<string[]>;
}

export class ConfiguredDiscovery implements Discovery {
  readonly list: string[];
  constructor(list: string[]) { this.list = list; }
  async personas(): Promise<string[]> {
    return this.list;
  }
}
