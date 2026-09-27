import { fetchCSRFToken } from "../auth/session-client";

/**
 * Same-origin browser-session client for the Cloud browser settings routes
 * (`/api/cloud-browser`). The viewer itself connects over a WebSocket with a
 * short-lived ticket from `viewerTicket`; nothing here reaches the browser
 * Worker directly.
 */

export interface CloudBrowserGrant {
  attachmentId: string;
  tabId: string;
  name: string;
  allowActions: boolean;
}

export interface CloudBrowserProfile {
  profileId: string;
  personaId: string;
  personaName: string;
  state: "sleeping" | "live" | "lost";
  incarnation: number;
  tabIds: string[];
  checkpointAt: string | null;
  checkpointBytes: number | null;
  grants: CloudBrowserGrant[];
}

export interface CloudBrowserPersona {
  personaId: string;
  name: string;
}

export interface CloudBrowserJev {
  configured: boolean;
  rejected: boolean;
}

export type CloudBrowserOverview =
  | { configured: false }
  | {
      configured: true;
      personas: CloudBrowserPersona[];
      profiles: CloudBrowserProfile[];
      jev: CloudBrowserJev;
      snapshotLimitBytes: number;
    };

export interface ViewerTicket {
  ticket: string;
  expiresAt: string;
  path: string;
}

export type CloudBrowserErrorCode =
  | "invalid_request"
  | "not_authorized"
  | "not_found"
  | "stale"
  | "unavailable"
  | "network";

export class CloudBrowserAPIError extends Error {
  readonly code: CloudBrowserErrorCode;
  constructor(code: CloudBrowserErrorCode) {
    super(code);
    this.name = "CloudBrowserAPIError";
    this.code = code;
  }
}

const BASE = "/api/cloud-browser";
const CODES: Record<string, CloudBrowserErrorCode> = {
  invalid_request: "invalid_request",
  not_authorized: "not_authorized",
  not_found: "not_found",
  stale: "stale",
  cloud_browser_unavailable: "unavailable",
};

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new CloudBrowserAPIError("unavailable");
  return value as Record<string, unknown>;
}

const text = (value: unknown) => (typeof value === "string" ? value : "");

function grant(value: unknown): CloudBrowserGrant {
  const g = record(value);
  return {
    attachmentId: text(g.attachment_id),
    tabId: text(g.tab_id),
    name: text(g.name),
    allowActions: g.allow_actions === true,
  };
}

function profile(value: unknown): CloudBrowserProfile {
  const p = record(value);
  const state = p.state === "live" || p.state === "lost" ? p.state : "sleeping";
  return {
    profileId: text(p.profile_id),
    personaId: text(p.persona_id),
    personaName: text(p.persona_name),
    state,
    incarnation: typeof p.incarnation === "number" ? p.incarnation : 0,
    tabIds: Array.isArray(p.tab_ids) ? p.tab_ids.filter((t) => typeof t === "string") : [],
    checkpointAt: typeof p.checkpoint_at === "string" ? p.checkpoint_at : null,
    checkpointBytes: typeof p.checkpoint_bytes === "number" ? p.checkpoint_bytes : null,
    grants: Array.isArray(p.grants) ? p.grants.map(grant) : [],
  };
}

function jev(value: unknown): CloudBrowserJev {
  const j = record(value ?? {});
  return { configured: j.configured === true, rejected: j.rejected === true };
}

export function parseOverview(value: unknown): CloudBrowserOverview {
  const o = record(value);
  if (o.configured !== true) return { configured: false };
  return {
    configured: true,
    personas: (Array.isArray(o.personas) ? o.personas : []).map((raw) => {
      const p = record(raw);
      return { personaId: text(p.persona_id), name: text(p.name) };
    }),
    profiles: (Array.isArray(o.profiles) ? o.profiles : []).map(profile),
    jev: jev(o.jev),
    snapshotLimitBytes: typeof o.snapshot_limit_bytes === "number" ? o.snapshot_limit_bytes : 0,
  };
}

export interface CloudBrowserAPI {
  overview(signal?: AbortSignal): Promise<CloudBrowserOverview>;
  createProfile(personaId: string): Promise<CloudBrowserProfile>;
  resetProfile(profileId: string): Promise<void>;
  viewerTicket(profileId: string): Promise<ViewerTicket>;
  grant(profileId: string, tabId: string, name: string, allowActions: boolean): Promise<CloudBrowserGrant>;
  revoke(profileId: string, attachmentId: string): Promise<void>;
  setJevKey(apiKey: string): Promise<CloudBrowserJev>;
  deleteJevKey(): Promise<void>;
}

export function createCloudBrowserAPI(
  fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
): CloudBrowserAPI {
  async function request(path: string, method = "GET", body?: unknown, signal?: AbortSignal): Promise<Record<string, unknown>> {
    let response: Response;
    try {
      const csrfToken = method === "GET" ? undefined : await fetchCSRFToken({ fetcher, signal });
      response = await fetcher(`${BASE}${path}`, {
        method,
        credentials: "include",
        cache: "no-store",
        headers: {
          Accept: "application/json",
          ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(15_000)]) : AbortSignal.timeout(15_000),
      });
    } catch {
      throw new CloudBrowserAPIError("network");
    }
    let payload: unknown = null;
    try {
      payload = await response.json();
    } catch {
      /* the status stays authoritative */
    }
    if (!response.ok) {
      const code = typeof payload === "object" && payload ? text((payload as { error?: unknown }).error) : "";
      throw new CloudBrowserAPIError(CODES[code] ?? (response.status === 403 ? "not_authorized" : "unavailable"));
    }
    return record(payload);
  }
  const id = encodeURIComponent;
  return {
    async overview(signal) {
      return parseOverview(await request("", "GET", undefined, signal));
    },
    async createProfile(personaId) {
      return profile((await request("/profiles", "POST", { persona_id: personaId })).profile);
    },
    async resetProfile(profileId) {
      await request(`/profiles/${id(profileId)}`, "DELETE");
    },
    async viewerTicket(profileId) {
      const t = await request(`/profiles/${id(profileId)}/viewer-ticket`, "POST");
      return { ticket: text(t.ticket), expiresAt: text(t.expires_at), path: text(t.path) || "/browser-cloud/viewer" };
    },
    async grant(profileId, tabId, name, allowActions) {
      return grant(
        (await request(`/profiles/${id(profileId)}/grants`, "POST", { tab_id: tabId, name, allow_actions: allowActions })).grant,
      );
    },
    async revoke(profileId, attachmentId) {
      await request(`/profiles/${id(profileId)}/grants/${id(attachmentId)}`, "DELETE");
    },
    async setJevKey(apiKey) {
      return jev((await request("/jev-key", "PUT", { api_key: apiKey })).jev);
    },
    async deleteJevKey() {
      await request("/jev-key", "DELETE");
    },
  };
}
