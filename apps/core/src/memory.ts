/**
 * Memory layer: asynchronous L1 preparation on the same journal the Go
 * agentstate service seals into chunks (docs/agent/memory.md,
 * memory-preparation-and-replacement-2026-09-08).
 *
 * Sealing, preparation, and application are separate durable states owned by
 * the state service. This module carries the core-side half: rendering the
 * sent context (raw tail + applied replacement blocks interleaved at their
 * original positions) and running the one-at-a-time preparation branch.
 *
 * The branch keeps the parent's prefix — system prompt, tool definitions,
 * rendered context — and the parent's model: it is the same individual
 * organizing its own memory, not a separate reviewer. Its tools are offered
 * but never executed. A finished candidate is shelved ('prepared');
 * only the state service applies it, and only while the live raw estimate
 * exceeds the 40k threshold. Events that arrived after a chunk was sealed —
 * corrections, new experiences — are outside its range and are never
 * touched by application.
 *
 * Lifecycle: a branch that ends with an answer or a genuine failure records
 * it (failures spend the chunk's attempts). A branch stopped by its host or
 * by losing the writer fence records nothing and makes no further model
 * call; the state service counts that claim as an interruption instead.
 */

import {
  ModelError,
  type ChatMessage,
  type ModelEvent,
  type ModelProvider,
  type ToolSpec,
} from "./provider.ts";
import type {
  ClaimedMemoryChunk,
  Event,
  MemoryBlock,
  MemoryChunk,
  OmittedMemory,
  OmittedRange,
} from "./types.ts";
import { FencedError, StateError, type StateClient } from "./state-client.ts";

/** A sealed L0 chunk cuts at a safe boundary once it reaches this estimate. */
export const L0_CHUNK_MIN_TOKENS = 10_000;
/** Prepared replacements apply only while live raw estimate exceeds this. */
export const L0_LIVE_LIMIT_TOKENS = 40_000;
/**
 * Default wall-clock bound on one preparation branch. A real-model
 * preparation has been observed at ~104s; the bound only catches a stream
 * that never ends, and it is recorded as a retryable failure.
 */
export const DEFAULT_MEMORY_PREPARATION_TIMEOUT_MS = 10 * 60_000;

/**
 * The L1 preparation instruction — carried over from the previous runtime's
 * compact-l0-to-l1 prompt, with locators adapted to the journal: chunk_seq
 * names the sealed range, seq names one stored record, and both open the
 * originals through conversation_history read. Same branch principle: the
 * fork inherits the parent's context, keeps events to the target's range,
 * distinguishes who said/did/observed what, and answers KEEP_UNCHANGED when
 * nothing can shrink without losing meaning or experience.
 */
export const COMPACT_L1_PROMPT =
  "今のあなたの文脈を引き継いだ分岐で、自分の記憶を整理する。後続の `compact_target` が今回置き換える対象を示す。各 `event` は親の送信文脈にある対象記録のコピーで、`seq` がその記録を識別する。\n\n" +
  "前後の文脈は対象部分を読み解くために使い、置換文に残す出来事や認識は対象の範囲に保つ。他のチャンクの出来事を取り込んだり、後から知ったことで当時の理解を書き換えたりしない。\n\n" +
  "まず、繰り返されるツール呼び出しの外枠や、ツール結果中の同じ本文の再掲を減らす。同じ発言や観測が再び起きた事実は残し、誰の言葉・行為・観測だったか、出来事の順序や時刻、呼び出しと結果の対応、途中で変わった理解が辿れるようにする。相手が言ったこと、自分が見聞きしたこと、自分の解釈や未確認のことは区別したまま残す。意味を担う言葉や数値についての経験を、単なる「資料を見た」「作業した」に置き換えない。\n\n" +
  "会話や共に過ごした出来事は、当面の仕事への有用性だけで選ばない。課題一覧、人物の固定した属性、教訓へ一律にまとめる必要はない。必要な内容は長く残してよく、決まった圧縮率・文字数・見出しはない。反省やメモを新たに作る手順でもない。\n\n" +
  "詳細を外部の記録へ預けると判断するなら、何をどこから読み返せるかが自分に分かる手掛かりを残す。対象の `chunk_seq` や各 `seq` は、通常の会話で `conversation_history` の `read` に指定して会話記録の保存済み原文を開ける。IDだけで内容を代用せず、取り戻せる内容と、今ここに残す理解を結び付ける。外部の資料を改めて開く場合は、当時見た内容へ戻ることと、更新後の内容を読むことを区別する。\n\n" +
  "出力は対象を置き換える文章だけとする。この分岐ではツールは実行されない。整理によって意味や経験を損なわずに減らせるものがなければ、`KEEP_UNCHANGED` だけを出力する。その場合は元の対象がそのまま保持される。";

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

/** Map one journal event to the model-visible message, or null for kinds
 * with no context rendering (e.g. internal bookkeeping). */
export function eventMessage(ev: Event): ChatMessage | null {
  const p = ev.payload;
  switch (ev.kind) {
    case "input_received": {
      const who =
        p.actor_kind === "schedule"
          ? "[scheduled wake]"
          : `[${String(p.actor_kind)}]`;
      return { role: "user", content: `${who} ${String(p.text ?? "")}` };
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
): ChatMessage[] {
  const items: { seq: number; message: ChatMessage }[] = [];
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
    items.push({ seq: b.first_seq, message: memoryBlockMessage(b) });
  }
  for (const x of extras) {
    items.push(x);
  }
  for (const ev of events) {
    const m = eventMessage(ev);
    if (m) items.push({ seq: ev.seq, message: m });
  }
  // Array sort is stable: ties keep the order pushed above.
  return items.sort((a, b) => a.seq - b.seq).map((i) => i.message);
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
  return Math.ceil((kind.length + 16 + JSON.stringify(payload).length) / 4);
}

/**
 * Drop the oldest raw journal records from a working view until the retained
 * raw estimate fits `budget`. Records are indivisible: an oversized record
 * drops whole or stays whole — nothing is split or truncated to satisfy the
 * number. Newest records are always kept.
 */
export function evictToBudget(
  events: Event[],
  budget: number,
): { kept: Event[]; evicted: Event[] } {
  const costs = events.map((e) => estEventTokens(e.kind, e.payload));
  let total = 0;
  for (const c of costs) total += c;
  let cut = 0;
  while (cut < events.length && total > budget) {
    total -= costs[cut] ?? 0;
    cut += 1;
  }
  return { kept: events.slice(cut), evicted: events.slice(0, cut) };
}

/**
 * The provider-capacity notice carried in a recovered working view: it names
 * exactly which journal records are out of this send — count, seq range and
 * recorded times — and how to reread them. It is not a summary, and it is
 * never written to the journal: the records stay in the canonical history.
 */
export function capacityNoticeMessage(evicted: Event[]): ChatMessage {
  const first = evicted[0];
  const last = evicted[evicted.length - 1];
  if (!first || !last) {
    throw new Error("capacity notice requires evicted records");
  }
  const source = JSON.stringify({
    operation: "read",
    from_seq: first.seq,
    limit: 5,
  });
  return {
    role: "user",
    content:
      "[Working-context capacity notice; not a new user message]\n" +
      `${evicted.length} earlier raw records from your private history — journal seq ${first.seq} through ${last.seq}, recorded ${first.created_at} through ${last.created_at} — ` +
      "are outside this working view because the provider rejected its size. They have not been summarized or deleted. " +
      "Their contents and any uncertainty about them remain in your original private history. " +
      `Reread with conversation_history(${source}), following next_after_seq while needed through sequence ${last.seq}. ` +
      "Do not treat this omission as evidence that those experiences were unimportant.",
  };
}

/** The compact_target user message: the sealed range's stored events,
 * verbatim, as journal records with their seq identities. */
export function compactTargetMessage(
  chunk: MemoryChunk,
  target: Event[],
): ChatMessage {
  return {
    role: "user",
    content:
      "compact_target\n" +
      JSON.stringify({
        chunk_seq: chunk.chunk_seq,
        layer: chunk.layer,
        range: { first_seq: chunk.first_seq, last_seq: chunk.last_seq },
        est_tokens: chunk.est_tokens,
        events: target.map((e) => ({
          seq: e.seq,
          kind: e.kind,
          created_at: e.created_at,
          payload: e.payload,
        })),
      }),
  };
}

/**
 * The branch's full request: the parent's own prefix kept as-is — the same
 * system prompt and the rendered journal context at claim time — with the
 * preparation instruction and the compact target appended at the end. The
 * branch is the same individual in the same context, not a target-only
 * summarizer.
 */
export function branchMessages(
  claimed: ClaimedMemoryChunk,
  system: string,
): ChatMessage[] {
  const messages: ChatMessage[] = [
    { role: "system", content: system },
    ...renderJournalContext(
      claimed.context.events ?? [],
      claimed.context.memory ?? [],
      claimed.context.omitted ?? null,
      claimed.context.memory_omitted ?? null,
    ),
  ];
  if (claimed.chunk) {
    const target = compactTargetMessage(claimed.chunk, claimed.target_events);
    messages.push({
      role: "user",
      content: `${COMPACT_L1_PROMPT}\n\n${target.content}`,
    });
  }
  return messages;
}

export interface MemoryPreparationDeps {
  personaId: string;
  generation: number;
  state: StateClient;
  provider: ModelProvider;
  contextLimit: number;
  /** The parent's system prompt — the branch keeps the parent prefix. */
  system: string;
  /** The parent's tool definitions; offered unchanged, never executed. */
  tools: ToolSpec[];
  /**
   * Aborted when the host stops or the writer fence is lost: the branch
   * ends at once, records nothing and makes no further model call.
   */
  signal?: AbortSignal;
  /** Wall-clock bound on the model call; exceeding it is a recorded failure. */
  timeoutMs?: number;
  log?: (msg: string, fields?: Record<string, unknown>) => void;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      reject(new DOMException("aborted", "AbortError"));
    });
  });
}

/** The model's preparation verdict for one claimed chunk. */
type PreparationOutcome =
  | { kind: "prepared"; replacement: string }
  | { kind: "kept" }
  | { kind: "failed"; error: string; retryable: boolean };

/**
 * Persist the branch's verdict. A claimed chunk must never be orphaned in
 * 'preparing' under a still-live generation — a transient failure to record
 * the outcome is retried with backoff until it lands, the process is
 * stopping, or the fence is lost (a later generation's recovery reseals it).
 */
async function recordMemoryOutcome(
  deps: MemoryPreparationDeps,
  chunkSeq: number,
  outcome: PreparationOutcome,
): Promise<void> {
  const { personaId, generation, state } = deps;
  const log = deps.log ?? (() => {});
  for (let attempt = 0; ; attempt++) {
    if (deps.signal?.aborted) return;
    try {
      if (outcome.kind === "prepared") {
        await state.completeMemoryChunk(personaId, generation, chunkSeq, {
          replacement: outcome.replacement,
        });
        log("memory candidate prepared", { chunk_seq: chunkSeq });
      } else if (outcome.kind === "kept") {
        await state.completeMemoryChunk(personaId, generation, chunkSeq, {
          keepUnchanged: true,
        });
        log("memory preparation kept originals", { chunk_seq: chunkSeq });
      } else {
        await state.failMemoryChunk(personaId, generation, chunkSeq, {
          error: outcome.error,
          retryable: outcome.retryable,
        });
        log("memory preparation failed", {
          chunk_seq: chunkSeq,
          retryable: outcome.retryable,
          error: outcome.error,
        });
      }
      return;
    } catch (e) {
      if (e instanceof FencedError) throw e;
      // A deterministic 400 completing 'prepared' means the text itself
      // cannot be stored — convert to a terminal fail so the chunk's real
      // state is recorded instead of looping on an impossible write.
      if (
        outcome.kind === "prepared" &&
        e instanceof StateError &&
        e.status === 400
      ) {
        outcome = {
          kind: "failed",
          error: `replacement rejected: ${e.message.slice(0, 1024)}`,
          retryable: false,
        };
        continue;
      }
      // Any other deterministic rejection (a conflicting stored state) can
      // never land by retrying: leave the chunk to the state service's
      // interruption accounting instead of holding the host in a loop.
      if (e instanceof StateError && e.status < 500 && e.status !== 429) {
        log("memory outcome rejected; not retried", {
          chunk_seq: chunkSeq,
          status: e.status,
          error: e.message.slice(0, 1024),
        });
        return;
      }
      const wait = Math.min(500 * 2 ** Math.min(attempt, 5), 10_000);
      try {
        await sleep(wait, deps.signal);
      } catch {
        return; // aborted
      }
    }
  }
}

/**
 * Iterate a provider stream, but stop waiting the moment `signal` aborts —
 * a provider that ignores cancellation must not hold the host's lifetime.
 */
async function* untilAborted(
  stream: AsyncIterable<ModelEvent>,
  signal: AbortSignal,
): AsyncGenerator<ModelEvent> {
  const it = stream[Symbol.asyncIterator]();
  let onAbort = () => {};
  const aborted = new Promise<never>((_, reject) => {
    onAbort = () => reject(new DOMException("aborted", "AbortError"));
    if (signal.aborted) onAbort();
    else signal.addEventListener("abort", onAbort, { once: true });
  });
  aborted.catch(() => {});
  try {
    for (;;) {
      const next = await Promise.race([it.next(), aborted]);
      if (next.done) return;
      yield next.value;
    }
  } catch (e) {
    // Let an abandoned provider finish on its own; nothing awaits it.
    void Promise.resolve(it.return?.()).catch(() => {});
    throw e;
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

/**
 * One preparation branch: claim the oldest sealable chunk, consult the model
 * with the parent's context, then record the verdict. Completing never
 * changes the sent context — application is the state service's separate,
 * threshold-gated step.
 *
 * Never throws except FencedError: every answer or genuine failure is
 * recorded on the chunk (retryable failures — provider errors, a stream that
 * ends incomplete, truncated or empty output, the timeout — return it to the
 * shelf with backoff; a spent budget marks it 'failed', visible rather than
 * silently skipped). A stop or fence loss records nothing.
 */
export async function runMemoryPreparation(
  deps: MemoryPreparationDeps,
): Promise<void> {
  const { personaId, generation, state, provider } = deps;
  const log = deps.log ?? (() => {});
  // A stopped or fenced writer claims nothing and spends no model call.
  if (deps.signal?.aborted) return;
  const claimed = await state.claimMemoryChunk(
    personaId,
    generation,
    deps.contextLimit,
  );
  const chunk = claimed.chunk;
  if (!chunk) return;
  if (deps.signal?.aborted) return;

  const timeoutMs = deps.timeoutMs ?? DEFAULT_MEMORY_PREPARATION_TIMEOUT_MS;
  const call = new AbortController();
  let timedOut = false;
  const onStop = () => call.abort();
  deps.signal?.addEventListener("abort", onStop, { once: true });
  const timer = setTimeout(() => {
    timedOut = true;
    call.abort();
  }, timeoutMs);
  let text = "";
  let toolCalls = 0;
  let usage: Record<string, unknown> | null = null;
  let streamError: unknown = null;
  try {
    const stream = provider.stream({
      personaId,
      turnId: `memory-l1-${chunk.chunk_seq}`,
      round: 0,
      messages: branchMessages(claimed, deps.system),
      tools: deps.tools,
      signal: call.signal,
    });
    for await (const ev of untilAborted(stream, call.signal)) {
      if (ev.type === "text") text += ev.delta;
      else if (ev.type === "tool_call") toolCalls++;
      else usage = ev.usage;
    }
  } catch (e) {
    streamError = e;
  } finally {
    clearTimeout(timer);
    deps.signal?.removeEventListener("abort", onStop);
  }

  if (deps.signal?.aborted) {
    // Host stop or fence loss: not a model failure. The claim stays
    // unresolved and the next claim or generation counts an interruption.
    log("memory preparation interrupted", { chunk_seq: chunk.chunk_seq });
    return;
  }
  const fail = (error: string, retryable: boolean) =>
    recordMemoryOutcome(deps, chunk.chunk_seq, {
      kind: "failed",
      error,
      retryable,
    });
  if (timedOut) {
    await fail(`preparation did not finish within ${timeoutMs}ms`, true);
    return;
  }
  if (streamError !== null) {
    const e = streamError;
    const retryable = !(e instanceof ModelError) || e.retryable;
    await fail(
      `model: ${(e instanceof Error ? e.message : String(e)).slice(0, 4 * 1024)}`,
      retryable,
    );
    return;
  }
  if (toolCalls > 0) {
    // Tools are offered for an identical prefix but never run here; an
    // output that tried to act instead of replacing is not adopted.
    await fail(
      `preparation output attempted ${toolCalls} tool call(s); tools are not executed in memory preparation`,
      true,
    );
    return;
  }
  // Same classification as the previous runtime's compactor: a response
  // that did not finish normally, or finished empty, is incomplete and
  // retried within the budget — never adopted, never terminal at once.
  if (usage === null) {
    await fail(
      "incomplete preparation response: stream ended without completion",
      true,
    );
    return;
  }
  const finish = usage.finish_reason;
  if (typeof finish === "string" && finish !== "stop") {
    await fail(
      `incomplete preparation response: finish_reason=${finish}`,
      true,
    );
    return;
  }
  const trimmed = text.trim();
  if (trimmed === "KEEP_UNCHANGED") {
    await recordMemoryOutcome(deps, chunk.chunk_seq, { kind: "kept" });
    return;
  }
  if (!trimmed) {
    await fail(
      "incomplete preparation response: empty replacement output",
      true,
    );
    return;
  }
  // PG text cannot hold NUL — strip it rather than let an un-storable
  // candidate loop at the persistence boundary.
  const replacement = trimmed.replace(/\u0000/g, "");
  await recordMemoryOutcome(deps, chunk.chunk_seq, {
    kind: "prepared",
    replacement,
  });
}
