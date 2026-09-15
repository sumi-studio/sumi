/**
 * Usage accounting helpers shared by the metering provider wrapper and
 * its callers.
 *
 * The fact/admission boundary lives in the state service: a metered call
 * reserves its priced estimate before any provider bytes are sent
 * (admitted under the writer generation) and records one immutable fact
 * when the call resolves (not fenced — the spend already happened). This
 * module carries the core-side vocabulary: the denial signal, the
 * pre-call token estimate, and the provider-report extraction that keeps
 * token categories distinct.
 */

import type { ChatMessage, ModelRequest, ToolSpec } from "./provider.ts";
import type { BudgetWait, UsageEstimate } from "./types.ts";

/**
 * Budget admission denied the call — no provider request was sent. This
 * is not a model failure: the caller parks the work durably (a turn
 * commits 'await' carrying the wait; a memory chunk reshelves) and the
 * state service resumes it when budget or funding changes.
 */
export class BudgetWaitError extends Error {
  readonly wait: BudgetWait;
  constructor(wait: BudgetWait) {
    super(
      `budget denied for ${wait.funding.kind}:${wait.funding.id}: ` +
        `needs ${wait.needed_minor} ${wait.currency}, ` +
        `${wait.remaining_minor} remaining of ${wait.limit_minor}`,
    );
    this.name = "BudgetWaitError";
    this.wait = wait;
  }
}

/**
 * Estimated input tokens for one request — the same ~4-bytes-per-token
 * family the state service and the journal estimate use. A declared
 * pre-call estimate for admission pricing only; the recorded fact keeps
 * the provider's own report distinct and never substitutes this number.
 */
export function estRequestInputTokens(
  messages: ChatMessage[],
  tools: ToolSpec[],
): number {
  let bytes = 0;
  for (const m of messages) {
    bytes += m.role.length + 16 + m.content.length;
    if (m.toolCalls) bytes += JSON.stringify(m.toolCalls).length;
  }
  if (tools.length) bytes += JSON.stringify(tools).length;
  return Math.ceil(bytes / 4);
}

/**
 * The admission estimate for one metered request. `outputBound` is the
 * maximum output the resolved provider will actually send on the wire
 * (its `outputBound()`); a call whose adapter sends no cap admits with
 * bounded=false so the reservation honestly bounds admission, not the
 * external bill. The input estimate is a size estimate, not a proven
 * upper bound — legitimate provider reports may exceed it.
 */
export function requestEstimate(
  request: ModelRequest,
  outputBound?: number,
): UsageEstimate {
  const est: UsageEstimate = {
    input_tokens: estRequestInputTokens(request.messages, request.tools),
  };
  if (typeof outputBound === "number" && outputBound > 0) {
    est.output_tokens_bound = outputBound;
  }
  return est;
}

const num = (v: unknown): number | null =>
  typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : null;

/**
 * Normalize a provider usage report into the ledger's non-overlapping
 * categories — input_tokens EXCLUDES cached_tokens on every wire, so the
 * stored categories never double-count and pricing is additive:
 *
 *   chat completions — prompt_tokens INCLUDES the cached subset reported
 *     at prompt_tokens_details.cached_tokens; subtract it.
 *   Responses        — same convention under input_tokens /
 *     input_tokens_details.cached_tokens.
 *   Anthropic        — input_tokens EXCLUDES cache_read_input_tokens and
 *     cache_creation_input_tokens (additive categories). Cache-read is
 *     the normalized cached category; cache-write input folds into
 *     input_tokens and prices at the input rate.
 *
 * Categories the provider did not report stay null — never zero — so a
 * partial report is distinguishable from an empty one. The raw report
 * rides along separately in quantities for provenance.
 */
export function reportedTokens(usage: Record<string, unknown>): {
  input: number | null;
  output: number | null;
  cached: number | null;
} {
  const cacheRead = num(usage.cache_read_input_tokens);
  const cacheWrite = num(usage.cache_creation_input_tokens);
  if (cacheRead !== null || cacheWrite !== null) {
    const base = num(usage.input_tokens);
    return {
      input:
        base === null && cacheWrite === null
          ? null
          : (base ?? 0) + (cacheWrite ?? 0),
      output: num(usage.output_tokens),
      cached: cacheRead,
    };
  }
  const details = usage.prompt_tokens_details ?? usage.input_tokens_details;
  const cachedDetail =
    details !== null && typeof details === "object"
      ? num((details as Record<string, unknown>).cached_tokens)
      : null;
  const cached = cachedDetail ?? num(usage.cached_tokens);
  const rawInput = num(usage.prompt_tokens ?? usage.input_tokens);
  return {
    input: rawInput === null ? null : Math.max(0, rawInput - (cached ?? 0)),
    output: num(usage.completion_tokens ?? usage.output_tokens),
    cached,
  };
}

/** A unique fact id for one actual provider call. A retry or a reduced
 * working-view resend is a genuinely additional call — it gets a fresh
 * id; only the redelivery of this call's record reuses it. */
export function newFactId(request: ModelRequest): string {
  return `${request.turnId}:${request.phase ?? "turn"}:${request.round}:${crypto.randomUUID()}`;
}
