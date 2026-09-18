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
   * Shared discovery seam (producer: apps/api job_discovery.go +
   * http.go). Both routes are service-credential only — this client
   * must be built with the runtime/admin token, never a persona token
   * (cross-persona visibility must not ride a single-persona grant).
   *
   * Fair cursor contract: rows strictly after (after_persona,
   * after_job) sort first; 'next' is present iff the page is full —
   * an absent next means the next call wraps to the start.
   */
  runnableJobs(kinds: string[], limit: number, afterPersona = "", afterJob = ""): Promise<JobPage> {
    const q = `kinds=${encodeURIComponent(kinds.join(","))}&limit=${limit}` +
      `&after_persona=${encodeURIComponent(afterPersona)}&after_job=${encodeURIComponent(afterJob)}`;
    return this.call("GET", `/internal/core/jobs/runnable?${q}`) as Promise<JobPage>;
  }

  attentionJobs(runnerID: string, kinds: string[], limit: number, afterPersona = "", afterJob = ""): Promise<JobPage> {
    const q = `runner_id=${encodeURIComponent(runnerID)}&kinds=${encodeURIComponent(kinds.join(","))}&limit=${limit}` +
      `&after_persona=${encodeURIComponent(afterPersona)}&after_job=${encodeURIComponent(afterJob)}`;
    return this.call("GET", `/internal/core/jobs/attention?${q}`) as Promise<JobPage>;
  }

  /**
   * Shared seam (wired by the producer): attach this runner's observed
   * outcome to a job whose claim expired and was swept to 'lost'.
   * 'lost' is an immutable verdict — CompleteJob conflicts on it — so
   * observed result/usage evidence attaches under result.observed_outcome:
   *
   *   POST .../jobs/{j}/lost-outcome
   *   { runner_id, observed_status, result, error }
   *
   *   - 200: outcome stored WITHOUT changing the 'lost' verdict, or an
   *     identical attach replayed (idempotent resend).
   *   - 403: runner_id is not the swept claim's recorded claimant —
   *     a stable runner identity matters (see main.ts runner-id).
   *   - 404: persona/job unknown.
   *   - 400: missing runner_id / malformed body.
   *   - 409: job is not 'lost' (a live claim completes via /complete),
   *     or divergent evidence conflicts with the recorded outcome.
   *   - 404/405/501 route: seam not deployed — caller keeps the
   *     evidence in its journal + usage facts, which are durable.
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

export interface JobPage {
  jobs: JobRow[];
  next?: { after_persona: string; after_job: string };
}

/** Adapter for "which personas may have runnable script jobs". */
export interface Discovery {
  personas(): Promise<string[]>;
}

/**
 * Production discovery: page the shared `GET /internal/core/jobs/runnable`
 * route and return the distinct persona ids on the current page, in
 * cursor order. The (after_persona, after_job) cursor persists across
 * polls and wraps when a page comes back short — so a queue deeper
 * than one page is walked fairly instead of re-reading the same
 * prefix, and a persona queued between polls is found without any
 * service configuration. Actual claiming still goes through the
 * per-persona claim route (kinds=["script"], backend unset → the
 * local/unstamped predicate — cloud-stamped work is never taken).
 */
export class SharedDiscovery implements Discovery {
  private afterPersona = "";
  private afterJob = "";
  private client: StateClient;
  private kinds: string[];
  private pageSize: number;
  constructor(client: StateClient, kinds = ["script"], pageSize = 64) {
    this.client = client;
    this.kinds = kinds;
    this.pageSize = pageSize;
  }
  async personas(): Promise<string[]> {
    const page = await this.client.runnableJobs(this.kinds, this.pageSize, this.afterPersona, this.afterJob);
    const personas = [...new Set(page.jobs.map((j) => j.persona_id))];
    if (page.next) {
      this.afterPersona = page.next.after_persona;
      this.afterJob = page.next.after_job;
    } else {
      this.afterPersona = "";
      this.afterJob = "";
    }
    return personas;
  }
}

/** Explicit persona list — local/dev filter only (SUMI_PERSONAS).
 *  Production discovery needs no persona configuration. */
export class ConfiguredDiscovery implements Discovery {
  readonly list: string[];
  constructor(list: string[]) { this.list = list; }
  async personas(): Promise<string[]> {
    return this.list;
  }
}
