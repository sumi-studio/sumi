import type { MemorySourceRange } from "./memory-branch.ts";
import type { ChatMessage } from "./provider.ts";
import type {
  Event,
  MemoryBlock,
  OmittedMemory,
  OmittedRange,
} from "./types.ts";
/** Journal rendering only. Compaction runs in the durable memory branch. */
export const L0_CHUNK_MIN_TOKENS = 10_000;
export const L0_LIVE_LIMIT_TOKENS = 40_000;
/** Render one applied memory block the way the previous runtime did: a
 * synthetic context note at the chunk's original position — not a
 * fabricated received message — carrying when its records were made (the
 * temporal anchor), their journal locator, and its re-read source. */
export function memoryBlockMessage(block: MemoryBlock): ChatMessage {
  const source = JSON.stringify({
    operation: "read",
    chunk_seq: block.chunk_seq,
    limit: 5,
  });
  return {
    role: "user",
    content:
      `[Memory fragment recorded ${block.first_time} through ${block.last_time}; journal seq ${block.first_seq} through ${block.last_seq}.]\n` +
      `Source: conversation_history(${source}). For further pages, pass next_after_seq as after_seq with the same chunk_seq.\n` +
      block.text,
  };
}

export type InputProvenance = {
  actorKind: string;
  actorName: unknown;
  surface: string;
  placeId: unknown;
  placeName: unknown;
  placeKind: unknown;
  messageId: unknown;
  workspaceId?: unknown;
  messageRevision?: unknown;
  sessionId?: unknown;
  status?: unknown;
  endReason?: unknown;
  exitCode?: unknown;
  exitSignal?: unknown;
  attention: string;
  /** "edited"/"deleted" when the input reports a change to an
   * already-delivered message rather than a new message. */
  change?: string;
};

const str = (v: unknown): string => (typeof v === "string" ? v : "");

/**
 * Render the "[who in where]" marker prefixing an input's text in model
 * context and in the current input. Provenance stays inside one bracket pair
 * (names are stripped of brackets) so directive-style content still parses
 * first. References identify the delivered workspace/message revision or
 * terminal session and its reported outcome. Names alone cannot distinguish
 * same-named places or terminals. The attention hint is part of the marker
 * — it informs, never mandates.
 */
export function inputMarker(p: InputProvenance): string {
  const clean = (s: string) => s.replace(/[[\]]/g, "");
  const name = clean(str(p.actorName));
  const who = name ? `${name} (${p.actorKind})` : p.actorKind;
  const placeLabel = clean(str(p.placeName)) || str(p.placeKind);
  const where = placeLabel ? ` in ${placeLabel}` : "";
  const refs =
    p.surface === "messaging"
      ? [
          str(p.workspaceId) && ` workspace_id=${clean(str(p.workspaceId))}`,
          str(p.placeId) && ` place_id=${clean(str(p.placeId))}`,
          str(p.messageId) && ` message_id=${clean(str(p.messageId))}`,
          Number.isSafeInteger(p.messageRevision)
            ? ` message_revision=${p.messageRevision}`
            : "",
        ].join("")
      : p.surface === "core_terminal_sessions"
        ? [
            str(p.sessionId) && ` session_id=${clean(str(p.sessionId))}`,
            str(p.status) && ` status=${clean(str(p.status))}`,
            str(p.endReason) && ` end_reason=${clean(str(p.endReason))}`,
            Number.isSafeInteger(p.exitCode) ? ` exit_code=${p.exitCode}` : "",
            str(p.exitSignal) && ` exit_signal=${clean(str(p.exitSignal))}`,
          ].join("")
        : "";
  const change =
    p.change === "edited"
      ? " — edited"
      : p.change === "deleted"
        ? " — deleted"
        : "";
  const hint =
    p.attention === "observe"
      ? " — fyi, no reply needed"
      : p.attention === "defer"
        ? " — deferred"
        : "";
  return `[${who}${where}${refs}${change}${hint}]`;
}

const formatByteSize = (n: number): string =>
  n >= 1_048_576
    ? `${(n / 1_048_576).toFixed(1)} MiB`
    : n >= 1024
      ? `${(n / 1024).toFixed(1)} KiB`
      : `${n} B`;

/**
 * Render the attachment metadata an input carries as readable lines, or ""
 * when there are none. Metadata is the delivered view — it never grants byte
 * access — so each line names the attachment_id the model would pass to
 * messaging.open_attachment (with the input's place_id and message_id) to
 * actually read the file.
 */
export function attachmentLines(raw: unknown): string {
  if (!Array.isArray(raw) || raw.length === 0) return "";
  const lines = raw.map((a) => {
    const o = (a ?? {}) as Record<string, unknown>;
    const name = str(o.filename) || "attachment";
    const mime = str(o.mime);
    const size =
      typeof o.size_bytes === "number"
        ? `, ${formatByteSize(o.size_bytes)}`
        : "";
    const spoiler = o.spoiler === true ? ", spoiler" : "";
    const alt = str(o.alt);
    const id = str(o.attachment_id);
    return (
      `- ${name}${mime ? ` (${mime}` : ""}${mime ? ")" : ""}${size}${spoiler}` +
      `${alt ? ` — ${alt}` : ""}${id ? ` [attachment_id=${id}]` : ""}`
    );
  });
  return `[${raw.length} attachment${raw.length === 1 ? "" : "s"} — bytes via messaging.open_attachment]\n${lines.join("\n")}`;
}

/**
 * The body text an input presents: its text plus any attachment lines. An
 * attachment-only message still produces a meaningful body, so the wake is
 * never content-free. "" when the input truly carries nothing readable —
 * callers keep their existing fallback for that case.
 */
export function inputBodyText(p: Record<string, unknown>): string {
  const text = typeof p.text === "string" ? p.text : "";
  const attachments = attachmentLines(p.attachments);
  if (text !== "" && attachments !== "") return `${text}\n${attachments}`;
  return attachments !== "" ? attachments : text;
}

const GAP_UNITS: [string, number][] = [
  ["day", 86_400_000],
  ["hour", 3_600_000],
  ["minute", 60_000],
  ["second", 1000],
];

/** A clock-reading difference in at most two units, "approximately" when
 * rounding dropped anything (docs/agent/incoming-event-time-2026-09-08.md). */
export function formatGap(ms: number): string {
  if (ms < 1000) return "less than a second";
  const lead = GAP_UNITS.findIndex(([, u]) => ms >= u);
  const step = GAP_UNITS[Math.min(lead + 1, GAP_UNITS.length - 1)]![1];
  const rounded = Math.round(ms / step) * step;
  // Rounding can carry into the next unit (59.6 s → 1 minute).
  const top = GAP_UNITS.findIndex(([, u]) => rounded >= u);
  let rest = rounded;
  const parts: string[] = [];
  for (const [name, u] of GAP_UNITS.slice(top, top + 2)) {
    const n = Math.floor(rest / u);
    rest -= n * u;
    if (n > 0) parts.push(`${n} ${name}${n === 1 ? "" : "s"}`);
  }
  return `${rounded === ms ? "" : "approximately "}${parts.join(" ")}`;
}

const utcSeconds = (ms: number) =>
  `${new Date(ms).toISOString().slice(0, 19).replace("T", " ")} UTC`;

/**
 * The receipt line that opens an incoming message: when Sumi's durable
 * intake accepted it and the gap since the previous incoming message, both
 * fixed at admission. "" when the receipt time is unknown. A journal
 * record from before receipts were pinned shows its journal time, labelled
 * as such, and no gap — the previous receipt was never recorded for it.
 */
export function receiptLine(
  receivedAt: unknown,
  previousReceivedAt: unknown,
  recordedAt?: unknown,
): string {
  const at = typeof receivedAt === "string" ? Date.parse(receivedAt) : NaN;
  if (Number.isNaN(at)) {
    const rec = typeof recordedAt === "string" ? Date.parse(recordedAt) : NaN;
    return Number.isNaN(rec) ? "" : `[Recorded ${utcSeconds(rec)}]`;
  }
  const prev =
    typeof previousReceivedAt === "string"
      ? Date.parse(previousReceivedAt)
      : NaN;
  if (Number.isNaN(prev)) return `[Received ${utcSeconds(at)}]`;
  const gap =
    at >= prev
      ? `${formatGap(at - prev)} since the previous incoming message`
      : `the receipt clock reads ${formatGap(prev - at)} earlier than for the previous incoming message`;
  return `[Received ${utcSeconds(at)}; ${gap}]`;
}

/** Inputs Sumi raises itself — not incoming messages, so they carry no
 * receipt line and never serve as the previous receipt (Go
 * previousReceiptCol). */
export const isInternalActor = (actorKind: unknown): boolean =>
  actorKind === "schedule" ||
  actorKind === "job" ||
  actorKind === "terminal" ||
  actorKind === "memory";

const FAILURE_REASONS: Record<string, string> = {
  no_model_connection: "no model connection was selected",
  oversize_plan: "the reply exceeded the size that can be recorded",
  model_reconnect_required:
    "the ChatGPT sign-in for the selected connection expired or was revoked",
  model_auth_rejected:
    "ChatGPT rejected the refreshed sign-in of the selected connection",
  model_connection_disabled:
    "the selected connection's kind is not enabled on this server",
  model_usage_limit: "the selected subscription's usage limit was reached",
};

/** Map one journal event to the model-visible message, or null for kinds
 * with no context rendering (e.g. internal bookkeeping). */
export function eventMessage(ev: Event): ChatMessage | null {
  const p = ev.payload;
  switch (ev.kind) {
    case "input_received": {
      const who =
        p.actor_kind === "schedule"
          ? "[scheduled wake]"
          : inputMarker({
              actorKind: String(p.actor_kind),
              actorName: p.actor_display,
              surface: str(p.source_surface),
              placeId: p.thread_id,
              placeName: p.place_name,
              placeKind: p.place_kind,
              messageId: p.message_id,
              workspaceId: p.workspace_id,
              messageRevision: p.message_revision,
              sessionId: p.session_id,
              status: p.status,
              endReason: p.end_reason,
              exitCode: p.exit_code,
              exitSignal: p.exit_signal,
              attention: str(p.attention),
              change: str(p.message_change),
            });
      const receipt = isInternalActor(p.actor_kind)
        ? ""
        : receiptLine(p.received_at, p.previous_received_at, ev.created_at);
      return {
        role: "user",
        content: `${receipt ? `${receipt}\n` : ""}${who} ${inputBodyText(p)}`,
      };
    }
    case "turn_failed": {
      // The requester was told; the secretary must be too, or an
      // unanswered input reads as a request still waiting for it.
      const why = FAILURE_REASONS[str(p.error_kind)];
      const lost =
        p.record_lost === true ? "; its record could not be stored" : "";
      return {
        role: "user",
        content: `[turn failed${why ? `: ${why}` : ""} — this turn ended without a completed reply${lost}]`,
      };
    }
    case "assistant_message":
      return { role: "assistant", content: String(p.text ?? "") };
    case "note":
      return { role: "assistant", content: `[note] ${String(p.text ?? "")}` };
    case "tool_result":
      // Flattened to assistant text: a bare role:"tool" message with no
      // preceding assistant tool_calls is rejected by chat-completions
      // providers. The journal keeps the structured record; the model gets
      // the result inline.
      return {
        role: "assistant",
        content: `[tool ${String(p.tool ?? "?")}] ${JSON.stringify(
          p.response ?? (p.error ? { error: p.error } : {}),
        )}`,
      };
    default:
      return null;
  }
}

/** A temporary notice that older raw records are outside the sent context —
 * where they are and how to open them. Not a summary of their content. */
export function omittedNoticeMessage(om: OmittedRange): ChatMessage {
  const source = JSON.stringify({
    operation: "read",
    from_seq: om.first_seq,
    limit: 5,
  });
  return {
    role: "user",
    content:
      `[${om.count} earlier records — journal seq ${om.first_seq} through ${om.last_seq}, recorded ${om.first_time} through ${om.last_time} — are outside your current context. ` +
      `They remain stored and are not summarized here. Open them with conversation_history(${source}).]`,
  };
}

/** A notice that older applied memory fragments are outside the sent
 * context because the fragments in context are bounded in size. It names
 * where they are and how to open their originals; it is not a summary. */
export function memoryOmittedNoticeMessage(om: OmittedMemory): ChatMessage {
  const source = JSON.stringify({
    operation: "read",
    from_seq: om.first_seq,
    limit: 5,
  });
  const fragments = om.count === 1 ? "fragment" : "fragments";
  return {
    role: "user",
    content:
      `[${om.count} older memory ${fragments} you organized — journal seq ${om.first_seq} through ${om.last_seq}, recorded ${om.first_time} through ${om.last_time} — are outside your current context because the memory fragments kept in context are limited in size. ` +
      `The fragments and their original records remain stored; nothing was summarized in their place. Open the original records with conversation_history(${source}).]`,
  };
}

/**
 * Render the journal as the model sees it: the raw event window plus applied
 * replacement blocks, each emitted at the position where its covered events
 * were. Raw records left outside the send cap and applied blocks left
 * outside the memory cap are each marked by one notice at their position.
 * Every item is ordered by the first journal seq it stands for, so the
 * original ordering holds. `extras` are synthetic items (e.g. a capacity
 * notice) rendered at their given journal position.
 */
export function renderJournalContext(
  events: Event[],
  memory: MemoryBlock[] = [],
  omitted: OmittedRange | null = null,
  memoryOmitted: OmittedMemory | null = null,
  extras: { seq: number; message: ChatMessage }[] = [],
  sourceRanges?: MemorySourceRange[],
): ChatMessage[] {
  const items: {
    seq: number;
    message: ChatMessage;
    source?: MemorySourceRange;
  }[] = [];
  if (memoryOmitted) {
    items.push({
      seq: memoryOmitted.first_seq,
      message: memoryOmittedNoticeMessage(memoryOmitted),
    });
  }
  if (omitted) {
    items.push({
      seq: omitted.first_seq,
      message: omittedNoticeMessage(omitted),
    });
  }
  for (const b of memory) {
    items.push({
      seq: b.first_seq,
      message: memoryBlockMessage(b),
      source: {
        first_seq: b.first_seq,
        last_seq: b.last_seq,
        layer: b.layer,
        chunk_seq: b.chunk_seq,
      },
    });
  }
  for (const x of extras) {
    items.push(x);
  }
  for (const ev of events) {
    const m = eventMessage(ev);
    if (m)
      items.push({
        seq: ev.seq,
        message: m,
        source: { first_seq: ev.seq, last_seq: ev.seq },
      });
    else sourceRanges?.push({ first_seq: ev.seq, last_seq: ev.seq });
  }
  // Array sort is stable: ties keep the order pushed above.
  return items
    .sort((a, b) => a.seq - b.seq)
    .map((i, index) => {
      if (i.source)
        sourceRanges?.push({ ...i.source, message_index: index + 1 });
      return i.message;
    });
}

/**
 * Estimated tokens of one journal record — the same ~4-bytes-per-token
 * accounting the state service uses (estPayloadTokens). Used here only to
 * size the temporary working view after a provider capacity refusal; it is
 * not provider billing and never writes durable state.
 */
export function estEventTokens(
  kind: string,
  payload: Record<string, unknown>,
): number {
  return Math.ceil(
    (kind.length +
      16 +
      new TextEncoder().encode(JSON.stringify(payload)).length) /
      4,
  );
}

/** Estimated tokens of a stored or rendered text — same ~4-bytes-per-token
 * family as estEventTokens. */
export function estTextTokens(text: string): number {
  return Math.ceil(new TextEncoder().encode(text).length / 4);
}
