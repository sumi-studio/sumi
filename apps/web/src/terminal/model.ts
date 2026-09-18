/**
 * Wire contract for the person-facing shared terminal
 * (`apps/api/internal/agentevents/browser_terminal.go`).
 *
 * A session is durable and owned by the persona: the human and the
 * secretary read the same absolute-offset scrollback and submit input
 * to the same ledger. This module only validates the wire shape; it
 * never invents state the server did not report.
 */

export const TERMINAL_STATUSES = [
  "requested",
  "claimed",
  "active",
  "ending",
  "interrupted",
  "ended",
  "lost",
] as const;

export type TerminalSessionStatus = (typeof TERMINAL_STATUSES)[number];

export interface TerminalSession {
  sessionId: string;
  name: string;
  mode: string;
  backend: string;
  status: string;
  requestedBy: string;
  outputBytes: number;
  outputBase: number;
  controlHolder: string;
  exitCode: number | null;
  exitSignal: string | null;
  endReason: string | null;
  createdAt: string;
  updatedAt: string;
  endedAt: string | null;
}

export type TerminalOutputChunk =
  | { kind: "data"; base: number; data: Uint8Array }
  | { kind: "gap"; base: number; gapTo: number | null };

export interface TerminalReadResult {
  session: TerminalSession;
  chunks: TerminalOutputChunk[];
  /** Absolute output offset the next read should resume from. */
  nextCursor: number;
}

export interface TerminalInputReceipt {
  inputId: string;
  seq: number;
  /** 'intended' means queued only — never present it as written. */
  status: string;
}

export function isLiveTerminalStatus(status: string): boolean {
  return (
    status === "requested" ||
    status === "claimed" ||
    status === "active" ||
    status === "ending" ||
    status === "interrupted"
  );
}

export function isTerminalAcceptingInput(session: { status: string }): boolean {
  return session.status === "active";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown): string {
  if (typeof value !== "string") throw new Error("invalid terminal payload");
  return value;
}

function asOffset(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("invalid terminal offset");
  }
  return value;
}

function optionalString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

export function parseTerminalSession(value: unknown): TerminalSession {
  if (!isRecord(value)) throw new Error("invalid terminal session");
  return {
    sessionId: asString(value.session_id),
    name: typeof value.name === "string" ? value.name : "",
    mode: typeof value.mode === "string" ? value.mode : "",
    backend: typeof value.backend === "string" ? value.backend : "",
    status: asString(value.status),
    requestedBy:
      typeof value.requested_by === "string" ? value.requested_by : "",
    outputBytes: asOffset(value.output_bytes ?? 0),
    outputBase: asOffset(value.output_base ?? 0),
    controlHolder:
      typeof value.control_holder === "string" ? value.control_holder : "",
    exitCode: Number.isSafeInteger(value.exit_code)
      ? (value.exit_code as number)
      : null,
    exitSignal: optionalString(value.exit_signal),
    endReason: optionalString(value.end_reason),
    createdAt: typeof value.created_at === "string" ? value.created_at : "",
    updatedAt: typeof value.updated_at === "string" ? value.updated_at : "",
    endedAt: optionalString(value.ended_at),
  };
}

export function decodeBase64(text: string): Uint8Array {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

export function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  const CHUNK = 0x8000;
  for (let index = 0; index < bytes.length; index += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(index, index + CHUNK));
  }
  return btoa(binary);
}

export function parseTerminalChunk(value: unknown): TerminalOutputChunk {
  if (!isRecord(value)) throw new Error("invalid terminal output chunk");
  const base = asOffset(value.base);
  if (value.kind === "data") {
    return { kind: "data", base, data: decodeBase64(asString(value.data)) };
  }
  if (value.kind === "gap") {
    return {
      kind: "gap",
      base,
      gapTo: Number.isSafeInteger(value.gap_to)
        ? (value.gap_to as number)
        : null,
    };
  }
  throw new Error("invalid terminal output chunk kind");
}

/** End offset of a chunk in the absolute output stream. */
export function terminalChunkEnd(chunk: TerminalOutputChunk): number {
  return chunk.kind === "gap"
    ? (chunk.gapTo ?? chunk.base)
    : chunk.base + chunk.data.length;
}

/**
 * The REST read response echoes the *requested* cursor, so the resume
 * point is derived here: the furthest absolute offset any returned
 * chunk reaches. With no chunks the stream has not advanced.
 */
export function parseTerminalRead(value: unknown): TerminalReadResult {
  if (!isRecord(value)) throw new Error("invalid terminal read response");
  const session = parseTerminalSession(value.session);
  const rawChunks = Array.isArray(value.chunks) ? value.chunks : [];
  const chunks = rawChunks.map(parseTerminalChunk);
  const cursor = asOffset(value.cursor ?? 0);
  let nextCursor = cursor;
  for (const chunk of chunks) {
    const end = terminalChunkEnd(chunk);
    if (end > nextCursor) nextCursor = end;
  }
  return { session, chunks, nextCursor };
}

export function parseTerminalInputReceipt(
  value: unknown,
): TerminalInputReceipt {
  if (!isRecord(value)) throw new Error("invalid terminal input receipt");
  return {
    inputId: asString(value.input_id),
    seq: asOffset(value.seq),
    status: asString(value.status),
  };
}

/** Status text shown to the person; server values map, unknown stays honest. */
export function terminalStatusLabel(status: string): string {
  switch (status) {
    case "requested":
      return "起動待ち";
    case "claimed":
      return "起動中";
    case "active":
      return "実行中";
    case "ending":
      return "終了処理中";
    case "interrupted":
      return "中断 — 再開待ち";
    case "ended":
      return "終了";
    case "lost":
      return "見失い";
    default:
      return status;
  }
}
