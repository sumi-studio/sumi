import type {
  BrowserAction,
  BrowserTabPort,
  PageBinding,
  TabRef,
} from "./contract.js";
import {
  type Admission,
  parseGoalRequest,
  privateRedactor,
  runGoal,
} from "./goal.js";
import type { JevClient } from "./jev.js";

export interface BrowserAttachment {
  attachment_id: string;
  persona_id: string;
  name: string;
  tab: TabRef;
  allow_actions: boolean;
  available: boolean;
  jev_available?: boolean;
}
export interface HostCredential {
  attachment: BrowserAttachment;
  host_token: string;
}
interface BrowserJob {
  job_id: string;
  claim_expires_at: string;
  request: {
    method: "observe" | "act" | "goal";
    attachment_id: string;
    tab: TabRef;
    binding?: PageBinding;
    action?: BrowserAction;
    goal?: string;
    inputs?: Record<string, string>;
    private_inputs?: string[];
    max_steps?: number;
  };
}
/** What the host window shows the person about a delegated goal. Progress is
 * the same scrubbed record the API stores; nothing here comes from the page's
 * scripts directly. */
export type GoalActivity =
  | { state: "running"; goal: string; progress?: Record<string, unknown> }
  | { state: "ended"; goal: string; outcome: string; status: string };

interface Receipt {
  job_id: string;
  status: "done" | "failed" | "cancelled";
  result: Record<string, unknown>;
  error: string;
}

/** A private transport to the API, such as a Workers VPC Service binding in
 * the Cloud browser host. The binding, not the URL, decides where requests
 * go, so its API origin may be plain HTTP. */
export type HostTransport = (url: string, init: RequestInit) => Promise<Response>;

function apiURL(base: string, path: string, privateTransport = false): string {
  const url = new URL(base);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/" ||
    (url.protocol !== "https:" &&
      !(
        url.protocol === "http:" &&
        (privateTransport ||
          ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))
      ))
  ) {
    throw new Error(
      "Browser host API must be an HTTPS origin (HTTP loopback is allowed for Local).",
    );
  }
  return new URL(path, url).href;
}
class HostHTTPError extends Error {
  constructor(readonly status: number) {
    super(`Browser host API returned ${status}`);
  }
}
async function request<T>(
  base: string,
  path: string,
  headers: Record<string, string>,
  body?: unknown,
  transport?: HostTransport,
): Promise<T> {
  const response = await (transport ?? fetch)(apiURL(base, path, !!transport), {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
    // Never follow redirects; a 3xx is not ok and fails like any error
    // ("error" is unavailable in Workers, where this code also runs).
    redirect: "manual",
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok) {
    await response.body?.cancel();
    throw new HostHTTPError(response.status);
  }
  const reader = response.body?.getReader();
  if (!reader) throw new Error("Empty browser host response");
  let size = 0;
  const chunks: Uint8Array[] = [];
  try {
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.length;
      if (size > 400_000) throw new Error("Browser host response too large");
      chunks.push(chunk.value);
    }
  } finally {
    await reader.cancel();
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8")) as T;
}

/** Call only in privileged main-process authentication code. Human credentials
 * are used once for attachment; only the separate tab-scoped token is retained.
 * The API's normal Sumi human authentication/CSRF checks remain in force. */
export async function attachBrowserTab(
  base: string,
  humanHeaders: Record<string, string>,
  input: {
    persona_id: string;
    name: string;
    tab: TabRef;
    allow_actions: boolean;
  },
): Promise<HostCredential> {
  return request(base, "/api/browser-tabs", humanHeaders, input);
}

/** Outbound-only bridge. The API authorizes and durably consumes each dispatch
 * once; this host never retries an action, only its exact completion receipt. */
export class BrowserHostAgent {
  private pending?: Receipt;
  private busy = false;
  private stopped = false;
  constructor(
    private readonly options: {
      apiOrigin: string;
      credential: HostCredential;
      browser: BrowserTabPort;
      tab: TabRef;
      /** Optional Jev operation layer for delegated goals. Without it the
       * direct observe/act path is unchanged and goals fail as not configured. */
      jev?: JevClient;
      minConfidence?: number;
      /** Host-window display of a running goal (see GoalActivity). */
      onGoal?: (activity: GoalActivity) => void;
      /** Private API transport (see HostTransport); global fetch otherwise. */
      transport?: HostTransport;
    },
  ) {
    apiURL(options.apiOrigin, "/", !!options.transport);
    if (!sameTab(options.credential.attachment.tab, options.tab))
      throw new Error("Attachment does not bind this live tab");
  }
  private readonly shutdown = new AbortController();
  private personStop?: AbortController;
  /** Set after Jev rejects the configured key: polls stop declaring Jev, so
   * browser.tabs and browser.goal stop offering it until the host restarts. */
  private jevRejected = false;
  stop(): void {
    this.stopped = true;
    this.shutdown.abort();
  }
  /** The person's Stop control. Ends the running goal before its next action;
   * an action already handed to the page completes and is recorded. */
  stopGoal(): boolean {
    if (!this.personStop || this.personStop.signal.aborted) return false;
    this.personStop.abort();
    return true;
  }
  get jevAvailable(): boolean {
    return !!this.options.jev && !this.jevRejected;
  }
  private async sendReceipt(): Promise<void> {
    if (!this.pending) return;
    try {
      await this.post("complete", this.pending);
      this.pending = undefined;
    } catch (error) {
      if (error instanceof HostHTTPError && [403, 409].includes(error.status)) {
        this.pending = undefined;
        // A terminal/expired job's receipt can conflict without revoking this
        // tab's grant. Drop only that receipt; never retry its browser action.
        // Authentication failure, unlike a per-job conflict, stops the host.
        if (error.status === 403) this.stopped = true;
      }
      throw error;
    }
  }
  private post<T>(
    operation: "poll" | "complete" | "progress",
    body?: unknown,
  ): Promise<T> {
    const c = this.options.credential;
    return request(
      this.options.apiOrigin,
      `/api/browser-host/tabs/${encodeURIComponent(c.attachment.attachment_id)}/${operation}`,
      { Authorization: `Bearer ${c.host_token}` },
      body,
      this.options.transport,
    );
  }
  async tick(): Promise<void> {
    if (this.busy) return;
    this.busy = true;
    try {
      if (this.pending) {
        await this.sendReceipt();
        return;
      }
      if (this.stopped) return;
      let response: { job: BrowserJob | null };
      try {
        // Declares whether this host can run delegated Jev goals.
        response = await this.post("poll", { jev: this.jevAvailable });
      } catch (error) {
        if (error instanceof HostHTTPError && error.status === 403)
          this.stopped = true;
        throw error;
      }
      const job = response.job;
      if (!job) return;
      let dispatched = false;
      try {
        const expiresAt = Date.parse(job.claim_expires_at);
        if (
          this.stopped ||
          !Number.isFinite(expiresAt) ||
          expiresAt <= Date.now() + 1000
        )
          throw new Error("Dispatch admission expired or host stopped");
        if (
          job.request.attachment_id !==
            this.options.credential.attachment.attachment_id ||
          !sameTab(job.request.tab, this.options.tab)
        )
          throw new Error("Dispatch does not bind the attached live tab");
        let value: unknown;
        if (job.request.method === "observe") {
          dispatched = true;
          value = await this.options.browser.observe(this.options.tab);
        } else if (
          job.request.method === "act" &&
          this.options.credential.attachment.allow_actions &&
          job.request.binding &&
          job.request.action
        ) {
          dispatched = true;
          value = await this.options.browser.act(
            this.options.tab,
            job.request.binding,
            job.request.action,
          );
        } else if (
          job.request.method === "goal" &&
          this.options.credential.attachment.allow_actions
        ) {
          const request = parseGoalRequest(job.request);
          // runGoal reports its own dispatch state; an unexpected throw after
          // this point may follow a landed action, so it is recorded as unknown.
          dispatched = true;
          const show = this.options.onGoal ?? (() => {});
          const goal = privateRedactor(request)(request.goal);
          this.personStop = new AbortController();
          show({ state: "running", goal });
          let receipt: Awaited<ReturnType<typeof runGoal>> | undefined;
          try {
            receipt = await runGoal({
              browser: this.options.browser,
              tab: this.options.tab,
              jev: this.jevAvailable ? this.options.jev : undefined,
              request,
              minConfidence: this.options.minConfidence,
              session: {
                signal: this.shutdown.signal,
                stop: this.personStop.signal,
                report: (progress) => {
                  show({ state: "running", goal, progress });
                  return this.progress(job.job_id, progress);
                },
              },
            });
          } finally {
            this.personStop = undefined;
            const value = receipt?.result.value as
              | { goal_outcome?: string }
              | undefined;
            show({
              state: "ended",
              goal,
              outcome: value?.goal_outcome ?? "host_error",
              status: receipt?.status ?? "failed",
            });
          }
          if (receipt.result.code === "jev_auth_failed")
            this.jevRejected = true;
          this.pending = { job_id: job.job_id, ...receipt };
        } else throw new Error("Unsupported or unauthorized browser request");
        this.pending ??= {
          job_id: job.job_id,
          status: "done",
          result: { dispatched, outcome: "returned", value },
          error: "",
        };
      } catch (error) {
        const code =
          error && typeof error === "object" && "code" in error
            ? String(error.code)
            : "host_unavailable";
        // A renderer/transport failure can follow a landed click. Conservatively
        // report unknown for dispatched failures; do not infer that nothing ran.
        this.pending = {
          job_id: job.job_id,
          status: "failed",
          result: {
            dispatched,
            outcome: dispatched ? "unknown" : "not_dispatched",
            code,
          },
          error:
            "Browser operation did not return successfully; inspect its recorded outcome before any new action",
        };
      }
      this.pending = withoutNUL(this.pending);
      await this.sendReceipt();
    } finally {
      this.busy = false;
    }
  }
  /** Claim renewal + progress for a running goal; the API answers with the
   * job's status so cancellation and revocation stop the next action. */
  private async progress(
    jobID: string,
    progress: Record<string, unknown>,
  ): Promise<Admission> {
    try {
      const { status } = await this.post<{ status: string }>("progress", {
        job_id: jobID,
        progress: withoutNUL(progress),
      });
      return status === "running" ? "continue" : "cancel";
    } catch (error) {
      if (error instanceof HostHTTPError && error.status === 403)
        return "revoked";
      if (error instanceof HostHTTPError && error.status === 409) return "lost";
      throw error;
    }
  }
  async run(
    signal: AbortSignal,
    onError: (error: unknown) => void = () => {},
  ): Promise<void> {
    while (!signal.aborted && !this.stopped) {
      try {
        await this.tick();
      } catch (error) {
        onError(error);
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    this.stop();
  }
}
// JSONB cannot store NUL, which arbitrary website text can contain.
function withoutNUL<T>(value: T): T {
  return JSON.parse(JSON.stringify(value), (_key, v) =>
    typeof v === "string" ? v.replaceAll("\0", "�") : v,
  ) as T;
}
function sameTab(a: TabRef, b: TabRef): boolean {
  return (
    a.runtimeId === b.runtimeId &&
    a.profileId === b.profileId &&
    a.tabId === b.tabId
  );
}
