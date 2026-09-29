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
  chunkSeq?: unknown;
  transition?: unknown;
  code?: unknown;
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
        : p.surface === "core_memory" && p.actorKind === "memory"
          ? [
              Number.isSafeInteger(p.chunkSeq) && Number(p.chunkSeq) > 0
                ? ` chunk_seq=${p.chunkSeq}`
                : "",
              str(p.transition) && ` transition=${clean(str(p.transition))}`,
              str(p.code) && ` code=${clean(str(p.code))}`,
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

/**
 * How a terminal failure reads in later context. It never claims that
 * nothing was said or done: a request can fail after its tool calls took
 * effect (a Messaging send, an edit), and those stay true. Each call is
 * journaled with its result at the claim that ran it, so they appear above
 * the marker even when the failing attempt's own records are not in hand;
 * when the closing commit could not be stored, what is missing is only
 * what the turn had not journaled yet — its closing reply.
 */
function turnFailedText(p: Record<string, unknown>): string {
  const why = FAILURE_REASONS[str(p.error_kind)];
  const head = `[turn failed${why ? `: ${why}` : ""} — `;
  if (p.record_lost === true) {
    return (
      head +
      "this turn's closing records could not be stored. Tool calls it ran are recorded above with their results as they happened " +
      "— for example, a message you sent stays sent — but any reply it ended with is not shown here]"
    );
  }
  return (
    head +
    "this request stopped here without finishing normally. " +
    "What is recorded above for it happened as shown — for example, a message you sent stays sent; nothing further runs for it]"
  );
}

/** Map one standalone journal event to the model-visible message, or null
 * for kinds with no standalone rendering. tool_call and tool_result are not
 * standalone: renderJournalContext renders them as the provider-native
 * pairs they were when the model decided them. */
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
              chunkSeq: p.chunk_seq,
              transition: p.transition,
              code: p.code,
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
    case "turn_failed":
      // The requester was told; the secretary must be too, or an
      // unanswered input reads as a request still waiting for it.
      return { role: "user", content: turnFailedText(p) };
    case "assistant_message":
      return { role: "assistant", content: String(p.text ?? "") };
    case "note":
      return { role: "assistant", content: `[note] ${String(p.text ?? "")}` };
    case "turn_paused":
      // An attempt that parked on usage budget, requeued after a transient
      // model error, or stopped with its host (recovery marks it) leaves
      // what it did so far in the journal; the request is not over, and its
      // resuming attempt continues from there without running those calls
      // again.
      return {
        role: "user",
        content:
          p.reason === "budget"
            ? "[This request paused here: its next model call is waiting for usage budget. " +
              "What is recorded above for it happened as shown; it continues when budget allows, " +
              "and the calls above are not run again.]"
            : p.reason === "interrupted"
              ? "[This request stopped here when its run was interrupted. " +
                "What is recorded above for it happened as shown; it resumes later, " +
                "and the calls above are not run again.]"
              : "[This request paused here after a temporary model error. " +
                "What is recorded above for it happened as shown; it is retried later, " +
                "and the calls above are not run again.]",
      };
    case "approval_requested": {
      // A parked call: decided, durably planned, and not run. Its outcome
      // (or denial) is journaled as an ordinary call/result pair by the
      // attempt that resumes it, so this is a status note, not a call.
      const approval = str(p.approval_id);
      return {
        role: "user",
        content:
          `[Approval requested: your ${str(p.route) || "elevated"} call ${str(p.tool) || "?"}` +
          `${str(p.call_id) ? ` (call_id ${str(p.call_id)})` : ""} is waiting for a human decision` +
          `${approval ? ` (approval_id ${approval})` : ""}; it has not run at this point. ` +
          `Requested input: ${JSON.stringify(p.request ?? {})}]`,
      };
    }
    case "approval_decided": {
      const by = str(p.decided_by_kind);
      return {
        role: "user",
        content:
          `[Approval decided: ${str(p.decision) || "?"}${by ? ` by ${by}` : ""} for your ` +
          `${str(p.route) || "elevated"} call ${str(p.tool) || "?"}` +
          `${str(p.approval_id) ? ` (approval_id ${str(p.approval_id)})` : ""}. ` +
          "What the call then did is recorded when its request resumes.]",
      };
    }
    default:
      return null;
  }
}

/**
 * The model-facing content of one recorded tool result — the same bytes
 * the model received live when the round's results were fed back in the
 * turn (secretary.ts feeds `response`, or `{error}` for a failed call), so
 * a later turn or a restart reads the same result.
 */
export function toolResultContent(p: Record<string, unknown>): string {
  return JSON.stringify(
    p.response ?? (p.error !== undefined ? { error: p.error } : {}),
  );
}

type RenderItem = {
  seq: number;
  message: ChatMessage;
  sources?: MemorySourceRange[];
};

/**
 * One model round as the journal recorded it: the round's deciding text
 * and the calls it decided, each with the result the model was fed.
 */
type RoundGroup = {
  seq: number;
  turnId: string;
  round: unknown;
  text: string | null;
  calls: { ev: Event; result: Event | null }[];
  /** Records journaled while a call awaited its result — what the call's
   * own effect recorded (a note) — rendered after the round's results. */
  after: RenderItem[];
};

/**
 * Render the journal records themselves, in seq order. A round's
 * assistant_message and the tool_call/tool_result records of the same
 * turn and round become the provider-native pair the model produced and
 * received live: one assistant message carrying the text and the decided
 * calls, then one tool message per result, in call order. The call's
 * arguments are what the secretary chose to do (the sent message body, the
 * edit); the result is what the effect actually returned (a receipt, or an
 * error). Neither is rewritten as the other's speech.
 *
 * Representing a call re-executes nothing: only calls streamed by the
 * current consultation are ever planned and claimed.
 *
 * Every assistant tool call is followed by its result, as every provider
 * wire requires. A record whose partner is outside the rendered view — the
 * raw window can begin between a call and its result — renders as an
 * explicit note instead of an unpaired native call or result.
 *
 * Call ids stay as recorded (a missing one is named after the call's
 * journal seq). They came from whichever provider decided them and can
 * repeat across turns; each provider adapter makes them valid and unique
 * for its own wire at serialization (disambiguateCallIds), where the
 * current turn's calls and opaque continuations are also in view.
 */
function renderEvents(events: Event[]): RenderItem[] {
  const out: RenderItem[] = [];
  let group: RoundGroup | null = null;

  const wireId = (ev: Event): string =>
    str(ev.payload.call_id) || `sumi_call_${ev.seq}`;
  const callLabel = (p: Record<string, unknown>) =>
    `${str(p.tool) || "?"}${str(p.call_id) ? ` (call_id ${str(p.call_id)})` : ""}`;

  const flush = () => {
    const g = group;
    group = null;
    if (!g) return;
    const paired = g.calls.filter((c) => c.result !== null);
    const ids = paired.map((c) => wireId(c.ev));
    if (g.text !== null || paired.length > 0) {
      out.push({
        seq: g.seq,
        sources: [
          ...(g.text !== null ? [g.seq] : []),
          ...paired.map((c) => c.ev.seq),
        ].map((seq) => ({ first_seq: seq, last_seq: seq })),
        message: {
          role: "assistant",
          content: g.text ?? "",
          ...(paired.length > 0
            ? {
                toolCalls: paired.map((c, i) => ({
                  id: ids[i]!,
                  name: str(c.ev.payload.tool),
                  route:
                    c.ev.payload.route === "elevated" ? "elevated" : "normal",
                  arguments: (c.ev.payload.request ?? {}) as Record<
                    string,
                    unknown
                  >,
                })),
              }
            : {}),
        },
      });
    }
    paired.forEach((c, i) => {
      out.push({
        seq: g.seq,
        sources: [{ first_seq: c.result!.seq, last_seq: c.result!.seq }],
        message: {
          role: "tool",
          toolCallId: ids[i]!,
          name: str(c.ev.payload.tool),
          content: toolResultContent(c.result!.payload),
        },
      });
    });
    for (const c of g.calls) {
      if (c.result !== null) continue;
      out.push({
        seq: g.seq,
        sources: [{ first_seq: c.ev.seq, last_seq: c.ev.seq }],
        message: {
          role: "user",
          content:
            `[Your ${c.ev.payload.route === "elevated" ? "elevated " : ""}call ${callLabel(c.ev.payload)} ` +
            `(journal seq ${c.ev.seq}) with input ${JSON.stringify(c.ev.payload.request ?? {})} ` +
            "has no recorded result in this context.]",
        },
      });
    }
    out.push(...g.after);
  };

  for (const ev of events) {
    const p = ev.payload;
    if (ev.kind === "assistant_message") {
      flush();
      group = {
        seq: ev.seq,
        turnId: ev.turn_id,
        round: p.round,
        text: String(p.text ?? ""),
        calls: [],
        after: [],
      };
      continue;
    }
    if (ev.kind === "tool_call") {
      const open = group as RoundGroup | null;
      if (!open || open.turnId !== ev.turn_id || open.round !== p.round) {
        flush();
        group = {
          seq: ev.seq,
          turnId: ev.turn_id,
          round: p.round,
          text: null,
          calls: [],
          after: [],
        };
      }
      group!.calls.push({ ev, result: null });
      continue;
    }
    if (ev.kind === "tool_result") {
      // The flat plan position identifies the call exactly; the model's
      // call_id is only unique when the provider made it so.
      const call = (group as RoundGroup | null)?.calls.find(
        (c) =>
          c.result === null &&
          (Number.isSafeInteger(p.call_index)
            ? c.ev.payload.call_index === p.call_index
            : c.ev.payload.call_id === p.call_id),
      );
      if (call) {
        call.result = ev;
        continue;
      }
      flush();
      out.push({
        seq: ev.seq,
        sources: [{ first_seq: ev.seq, last_seq: ev.seq }],
        message: {
          role: "user",
          content:
            `[Result of your earlier call ${callLabel(p)} (journal seq ${ev.seq}); ` +
            `the call itself is outside this context]\n${toolResultContent(p)}`,
        },
      });
      continue;
    }
    const m = eventMessage(ev);
    const open = group as RoundGroup | null;
    if (open?.calls.some((c) => c.result === null)) {
      // An effect records inside its claim, between its call and its
      // result (a journal.note): it follows the round's results rather
      // than splitting a call from its result.
      if (m)
        open.after.push({
          seq: ev.seq,
          message: m,
          sources: [{ first_seq: ev.seq, last_seq: ev.seq }],
        });
      continue;
    }
    flush();
    if (m)
      out.push({
        seq: ev.seq,
        message: m,
        sources: [{ first_seq: ev.seq, last_seq: ev.seq }],
      });
  }
  flush();
  return out;
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
    sources?: MemorySourceRange[];
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
      sources: [
        {
          first_seq: b.first_seq,
          last_seq: b.last_seq,
          layer: b.layer,
          chunk_seq: b.chunk_seq,
        },
      ],
    });
  }
  for (const x of extras) {
    items.push(x);
  }
  const rendered = renderEvents(events);
  items.push(...rendered);
  const mapped = new Set(
    rendered.flatMap((i) => (i.sources ?? []).map((r) => r.first_seq)),
  );
  for (const ev of events)
    if (!mapped.has(ev.seq))
      sourceRanges?.push({ first_seq: ev.seq, last_seq: ev.seq });
  // Stable order keeps each native assistant call followed by its results.
  return items
    .sort((a, b) => a.seq - b.seq)
    .map((item, index) => {
      for (const source of item.sources ?? [])
        sourceRanges?.push({ ...source, message_index: index + 1 });
      return item.message;
    });
}

/**
 * Estimated tokens of one journal record — the same ~4-bytes-per-token
 * accounting the state service uses (estPayloadTokens). Includes serialized tool arguments and results; this is a capacity
 * heuristic, not provider billing.
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
