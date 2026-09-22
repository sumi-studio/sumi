import type {
  BrowserAction,
  BrowserTabPort,
  PageBinding,
  TabRef,
} from "./contract.js";

export interface BrowserAttachment {
  attachment_id: string;
  persona_id: string;
  name: string;
  tab: TabRef;
  allow_actions: boolean;
  available: boolean;
}
export interface HostCredential {
  attachment: BrowserAttachment;
  host_token: string;
}
interface BrowserJob {
  job_id: string;
  claim_expires_at: string;
  request: {
    method: "observe" | "act";
    attachment_id: string;
    tab: TabRef;
    binding?: PageBinding;
    action?: BrowserAction;
  };
}
interface Receipt {
  job_id: string;
  status: "done" | "failed";
  result: Record<string, unknown>;
  error: string;
}

function apiURL(base: string, path: string): string {
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
        ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname)
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
): Promise<T> {
  const response = await fetch(apiURL(base, path), {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
    redirect: "error",
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
    },
  ) {
    apiURL(options.apiOrigin, "/");
    if (!sameTab(options.credential.attachment.tab, options.tab))
      throw new Error("Attachment does not bind this live tab");
  }
  stop(): void {
    this.stopped = true;
  }
  private async sendReceipt(): Promise<void> {
    if (!this.pending) return;
    try {
      await this.post("complete", this.pending);
      this.pending = undefined;
    } catch (error) {
      if (error instanceof HostHTTPError && [403, 409].includes(error.status)) {
        this.pending = undefined;
        this.stopped = true;
      }
      throw error;
    }
  }
  private post<T>(operation: "poll" | "complete", body?: unknown): Promise<T> {
    const c = this.options.credential;
    return request(
      this.options.apiOrigin,
      `/api/browser-host/tabs/${encodeURIComponent(c.attachment.attachment_id)}/${operation}`,
      { Authorization: `Bearer ${c.host_token}` },
      body,
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
        response = await this.post("poll");
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
        } else throw new Error("Unsupported or unauthorized browser request");
        this.pending = {
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
      // JSONB cannot store NUL, which arbitrary website text can contain.
      this.pending = JSON.parse(JSON.stringify(this.pending), (_key, value) =>
        typeof value === "string" ? value.replaceAll("\0", "�") : value,
      ) as Receipt;
      await this.sendReceipt();
    } finally {
      this.busy = false;
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
function sameTab(a: TabRef, b: TabRef): boolean {
  return (
    a.runtimeId === b.runtimeId &&
    a.profileId === b.profileId &&
    a.tabId === b.tabId
  );
}
