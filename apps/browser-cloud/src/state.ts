import type { HostCredential } from "@sumi/desktop/browser-host";
import type { Snapshot } from "./remote-browser.ts";

export interface FetcherLike {
  fetch(input: string, init?: RequestInit): Promise<Response>;
}

export interface HostSession {
  profile_id: string;
  persona_id: string;
  enabled: boolean;
  incarnation: number;
  attachments: HostCredential[];
  snapshot?: Snapshot;
  snapshot_seq: number;
  snapshot_at?: string;
  snapshot_version: number;
  jev_key?: string;
  /** Which saved key `jev_key` is; a rejection names it so that a key
   * saved later is never marked rejected by an older key's failure. */
  jev_key_version?: number;
}

export class StateError extends Error {
  readonly status: number;
  constructor(status: number) {
    super(`Sumi API answered ${status}`);
    this.status = status;
  }
}

/** The browser Worker's calls to the Sumi API's Cloud browser host routes.
 * In Cloud they travel through the Workers VPC Service binding (SUMI_STATE);
 * `base` is then only the Host header and path base. */
export class StateClient {
  private readonly base: string;
  private readonly token: string;
  private readonly transport?: FetcherLike;

  constructor(base: string, token: string, transport?: FetcherLike) {
    this.base = base;
    this.token = token;
    this.transport = transport;
  }

  /** For BrowserHostAgent: the same private route to the API. */
  get hostTransport(): ((url: string, init: RequestInit) => Promise<Response>) | undefined {
    const transport = this.transport;
    return transport ? (url, init) => transport.fetch(url, init) : undefined;
  }

  private async call<T>(profile: string, op: string, body: unknown): Promise<T> {
    const url = new URL(`/api/cloud-browser-host/profiles/${encodeURIComponent(profile)}/${op}`, this.base).href;
    const init: RequestInit = {
      method: "POST",
      headers: { Authorization: `Bearer ${this.token}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
      // Never follow redirects; a 3xx is not ok and fails like any error
      // ("error" is unavailable in Workers, where this code also runs).
      redirect: "manual",
      signal: AbortSignal.timeout(15_000),
    };
    const response = await (this.transport ? this.transport.fetch(url, init) : fetch(url, init));
    if (!response.ok) {
      await response.body?.cancel();
      throw new StateError(response.status);
    }
    return (await response.json()) as T;
  }

  begin(profile: string, fresh: boolean, incarnation = 0): Promise<HostSession> {
    return this.call(profile, "begin", { fresh, incarnation });
  }

  refresh(profile: string, incarnation: number, known: string[]): Promise<HostSession> {
    return this.call(profile, "refresh", { incarnation, known });
  }

  saveSnapshot(profile: string, incarnation: number, seq: number, snapshot: Snapshot): Promise<{ saved_at: string }> {
    return this.call(profile, "snapshot", {
      incarnation,
      seq,
      version: snapshot.v,
      tab_ids: snapshot.tabs.map((t) => t.slot),
      snapshot,
    });
  }

  setState(profile: string, incarnation: number, state: "sleeping" | "live" | "lost"): Promise<unknown> {
    return this.call(profile, "state", { incarnation, state });
  }

  jevRejected(profile: string, keyVersion: number): Promise<unknown> {
    return this.call(profile, "jev-rejected", { key_version: keyVersion });
  }
}
