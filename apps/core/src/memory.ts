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
 */

import {
  ModelError,
  type ChatMessage,
  type ModelProvider,
  type ToolSpec,
} from "./provider.ts";
import type {
  ClaimedMemoryChunk,
  Event,
  MemoryBlock,
  MemoryChunk,
  OmittedRange,
} from "./types.ts";
import { FencedError, StateError, type StateClient } from "./state-client.ts";

/** A sealed L0 chunk cuts at a safe boundary once it reaches this estimate. */
export const L0_CHUNK_MIN_TOKENS = 10_000;
/** Prepared replacements apply only while live raw estimate exceeds this. */
export const L0_LIVE_LIMIT_TOKENS = 40_000;

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
 * fabricated received message — carrying its re-read source. */
export function memoryBlockMessage(block: MemoryBlock): ChatMessage {
  const source = JSON.stringify({
    operation: "read",
    chunk_seq: block.chunk_seq,
    limit: 5,
  });
  return {
    role: "user",
    content:
      `[Memory fragment — journal seq ${block.first_seq} through ${block.last_seq}.]\n` +
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

/**
 * Render the journal as the model sees it: the raw event window plus applied
 * replacement blocks, each emitted at the position where its covered events
 * were (before the first following event). Blocks whose range ends before
 * the raw window lead the context; a block covering the newest events
 * trails it — in every case the original ordering holds. Raw records left
 * outside the send cap are marked by one notice at their position.
 */
export function renderJournalContext(
  events: Event[],
  memory: MemoryBlock[] = [],
  omitted: OmittedRange | null = null,
): ChatMessage[] {
  const messages: ChatMessage[] = [];
  let bi = 0;
  const emitBlock = () => {
    const b = memory[bi];
    if (!b) return;
    bi++;
    messages.push(memoryBlockMessage(b));
  };
  if (omitted) {
    while (
      bi < memory.length &&
      (memory[bi]?.last_seq ?? 0) < omitted.first_seq
    ) {
      emitBlock();
    }
    messages.push(omittedNoticeMessage(omitted));
  }
  for (const ev of events) {
    while (bi < memory.length && (memory[bi]?.last_seq ?? 0) < ev.seq) {
      emitBlock();
    }
    const m = eventMessage(ev);
    if (m) messages.push(m);
  }
  while (bi < memory.length) {
    emitBlock();
  }
  return messages;
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
      claimed.context.events,
      claimed.context.memory ?? [],
      claimed.context.omitted ?? null,
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
  signal?: AbortSignal;
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
 * One preparation branch: claim the oldest sealable chunk, consult the model
 * with the parent's context, then record the verdict. Completing never
 * changes the sent context — application is the state service's separate,
 * threshold-gated step.
 *
 * Never throws except FencedError: every other failure is recorded on the
 * chunk (retryable ones return it to the shelf with backoff; a spent budget
 * marks it 'failed', visible rather than silently skipped).
 */
export async function runMemoryPreparation(
  deps: MemoryPreparationDeps,
): Promise<void> {
  const { personaId, generation, state, provider } = deps;
  const claimed = await state.claimMemoryChunk(
    personaId,
    generation,
    deps.contextLimit,
  );
  const chunk = claimed.chunk;
  if (!chunk) return;
  let text = "";
  let toolCalls = 0;
  try {
    for await (const ev of provider.stream({
      personaId,
      turnId: `memory-l1-${chunk.chunk_seq}`,
      round: 0,
      messages: branchMessages(claimed, deps.system),
      tools: deps.tools,
      signal: deps.signal,
    })) {
      if (ev.type === "text") text += ev.delta;
      else if (ev.type === "tool_call") toolCalls++;
    }
  } catch (e) {
    // A stop aborts the stream; the chunk stays claimed and the next claim
    // or generation re-prepares it — an abort is not a model failure.
    if (deps.signal?.aborted) return;
    const retryable = !(e instanceof ModelError) || e.retryable;
    await recordMemoryOutcome(deps, chunk.chunk_seq, {
      kind: "failed",
      error: `model: ${(e instanceof Error ? e.message : String(e)).slice(0, 4 * 1024)}`,
      retryable,
    });
    return;
  }
  if (deps.signal?.aborted) return;
  if (toolCalls > 0) {
    // Tools are offered for an identical prefix but never run here; an
    // output that tried to act instead of replacing is not adopted.
    await recordMemoryOutcome(deps, chunk.chunk_seq, {
      kind: "failed",
      error: `preparation output attempted ${toolCalls} tool call(s); tools are not executed in memory preparation`,
      retryable: true,
    });
    return;
  }
  const trimmed = text.trim();
  if (trimmed === "KEEP_UNCHANGED") {
    await recordMemoryOutcome(deps, chunk.chunk_seq, { kind: "kept" });
    return;
  }
  if (!trimmed) {
    await recordMemoryOutcome(deps, chunk.chunk_seq, {
      kind: "failed",
      error: "empty replacement output",
      retryable: false,
    });
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
