import type { Place } from "../model";
import { requireActiveMessagingBoundary, scopedMessagingPath } from "../scope";
import type { CallParticipant, CallState, CallTicket } from "./model";

const REQUEST_TIMEOUT_MS = 15_000;

export class CallAPIError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, status: number) {
    super(code);
    this.name = "CallAPIError";
    this.code = code;
    this.status = status;
  }

  get unavailable(): boolean {
    return (
      this.status === 503 ||
      (this.status === 404 && this.code === "call_request_failed")
    );
  }
}

function placeID(place: Place): string {
  return place.kind === "channel" ? place.channelId : place.dmId;
}

async function request(
  path: string,
  options: { method?: string; body?: unknown } = {},
): Promise<unknown> {
  const boundary = requireActiveMessagingBoundary();
  const response = await fetch(scopedMessagingPath(path, boundary.scope), {
    method: options.method ?? "GET",
    credentials: "include",
    cache: "no-store",
    headers: {
      Accept: "application/json",
      ...(options.body === undefined
        ? {}
        : { "Content-Type": "application/json" }),
    },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
    signal: AbortSignal.any([
      boundary.signal,
      AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    ]),
  });
  if (!response.ok) {
    let code = "call_request_failed";
    try {
      const failure = (await response.json()) as Record<string, unknown>;
      if (typeof failure.error === "string") code = failure.error;
    } catch {
      // HTTP status remains the non-sensitive fallback.
    }
    throw new CallAPIError(code, response.status);
  }
  if (response.status === 204) return null;
  return response.json() as Promise<unknown>;
}

export async function fetchCallTicket(place: Place): Promise<CallTicket> {
  const body = asRecord(
    await request(
      `/messaging/places/${encodeURIComponent(placeID(place))}/call/token`,
      { method: "POST", body: {} },
    ),
  );
  return {
    url: asString(body.url),
    token: asString(body.token),
    room: asString(body.room),
    identity: asString(body.identity),
  };
}

export async function fetchCallStates(): Promise<CallState[]> {
  const body = asRecord(await request("/messaging/calls"));
  return asArray(body.calls).map(parseCallState);
}

export function parseCallState(value: unknown): CallState {
  const wire = asRecord(value);
  return {
    place: parsePlace(wire.place),
    active: wire.active === true,
    startedAt: wire.started_at == null ? null : asTimestamp(wire.started_at),
    participants: asArray(wire.participants).map(parseCallParticipant),
  };
}

function parseCallParticipant(value: unknown): CallParticipant {
  const wire = asRecord(value);
  return {
    participant: parseParticipant(wire.participant),
    joinedAt: wire.joined_at == null ? 0 : asTimestamp(wire.joined_at),
    screenShare: wire.screen_share === true,
  };
}

function parseParticipant(value: unknown) {
  const wire = asRecord(value);
  const kind = asString(wire.kind);
  if (kind === "human") {
    return { kind, humanId: asString(wire.human_id) } as const;
  }
  if (kind === "personality_agent") {
    return {
      kind,
      personalityAgentId: asString(wire.personality_agent_id),
    } as const;
  }
  throw new Error("invalid call participant");
}

function parsePlace(value: unknown): Place {
  const wire = asRecord(value);
  const kind = asString(wire.kind);
  if (kind === "channel") return { kind, channelId: asString(wire.channel_id) };
  if (kind === "dm" || kind === "group_dm") {
    return { kind, dmId: asString(wire.dm_id) };
  }
  throw new Error("invalid call place");
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("invalid call response");
  }
  return value as Record<string, unknown>;
}

function asArray(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("invalid call response");
  return value;
}

function asString(value: unknown): string {
  if (typeof value !== "string" || value === "") {
    throw new Error("invalid call response");
  }
  return value;
}

function asTimestamp(value: unknown): number {
  const parsed = Date.parse(asString(value));
  if (!Number.isFinite(parsed)) throw new Error("invalid call timestamp");
  return parsed;
}
