import { fetchCSRFToken } from "../auth/session-client";

export type UsageFundingKind = "connection" | "sumi";

export interface UsageBudget {
  fundingKind: UsageFundingKind;
  fundingId: string;
  limitMinor: number;
  currency: string;
  rateInputPerMTok: number;
  rateOutputPerMTok: number;
  rateCachedPerMTok?: number;
  pricingRevision: string;
  updatedAt: string;
  spentMinor: number;
  heldMinor: number;
  remainingMinor: number;
}

export interface UsageTotals {
  calls: number;
  unknownCalls: number;
  unpricedCalls: number;
  inputTokens: number;
  outputTokens: number;
  cachedTokens: number;
  /** Recorded spend per currency code (minor units) — a funding source can
   *  legitimately accumulate cost in more than one currency across rate-card
   *  edits, so the totals never collapse to a single number. */
  costs: Record<string, number>;
}

export interface UsageFact {
  personaId: string;
  factId: string;
  phase: "turn" | "memory";
  fundingKind: string;
  fundingId: string;
  model?: string;
  status: "reported" | "unknown" | "unrecorded" | "not_sent";
  inputTokens?: number;
  outputTokens?: number;
  cachedTokens?: number;
  costMinor?: number;
  currency?: string;
  costBasis?: string;
  pricingRevision?: string;
  recordedAt: string;
}

export interface UsageSource {
  kind: UsageFundingKind;
  id: string;
  name?: string;
  model?: string;
  selected: boolean;
  grant: boolean;
  budget?: UsageBudget;
  totals: UsageTotals;
  recent: UsageFact[];
}

export interface UsageWait {
  personaId: string;
  inputId: string;
  fundingKind: string;
  fundingId: string;
  neededMinor: number;
  currency: string;
  createdAt: string;
}

export interface UsageOverview {
  sources: UsageSource[];
  waits: UsageWait[];
}

export interface UsageBudgetInput {
  limitMinor: number;
  currency: string;
  rateInputPerMTok: number;
  rateOutputPerMTok: number;
  rateCachedPerMTok?: number;
  pricingRevision: string;
}

export interface UsageAPI {
  overview(signal: AbortSignal): Promise<UsageOverview>;
  setBudget(
    kind: UsageFundingKind,
    id: string,
    budget: UsageBudgetInput,
    signal: AbortSignal,
  ): Promise<UsageBudget>;
  clearBudget(
    kind: UsageFundingKind,
    id: string,
    signal: AbortSignal,
  ): Promise<void>;
}

const CHECK_FAILED = "利用状況を確認できませんでした。";

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(CHECK_FAILED);
  }
  return value as Record<string, unknown>;
}

function num(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new Error(CHECK_FAILED);
  }
  return value;
}

function optNum(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value)
    ? value
    : undefined;
}

function optText(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

function totals(value: unknown): UsageTotals {
  const row = record(value);
  const costs: Record<string, number> = {};
  for (const [currency, minor] of Object.entries(record(row.costs))) {
    costs[currency] = num(minor);
  }
  return {
    calls: num(row.calls),
    unknownCalls: num(row.unknown_calls),
    unpricedCalls: num(row.unpriced_calls),
    inputTokens: num(row.input_tokens),
    outputTokens: num(row.output_tokens),
    cachedTokens: num(row.cached_tokens),
    costs,
  };
}

function budget(value: unknown): UsageBudget {
  const row = record(value);
  const kind = row.funding_kind;
  if (kind !== "connection" && kind !== "sumi") throw new Error(CHECK_FAILED);
  return {
    fundingKind: kind,
    fundingId: String(row.funding_id),
    limitMinor: num(row.limit_minor),
    currency: String(row.currency),
    rateInputPerMTok: num(row.rate_input_per_mtok),
    rateOutputPerMTok: num(row.rate_output_per_mtok),
    rateCachedPerMTok: optNum(row.rate_cached_per_mtok),
    pricingRevision: String(row.pricing_revision),
    updatedAt: String(row.updated_at),
    spentMinor: num(row.spent_minor),
    heldMinor: num(row.held_minor),
    remainingMinor: num(row.remaining_minor),
  };
}

function fact(value: unknown): UsageFact {
  const row = record(value);
  const funding = record(row.funding);
  if (
    row.status !== "reported" &&
    row.status !== "unknown" &&
    row.status !== "unrecorded" &&
    row.status !== "not_sent"
  ) {
    throw new Error(CHECK_FAILED);
  }
  return {
    personaId: String(row.persona_id),
    factId: String(row.fact_id),
    phase: row.phase === "memory" ? "memory" : "turn",
    fundingKind: String(funding.kind),
    fundingId: String(funding.id),
    model: optText(funding.model),
    status: row.status,
    inputTokens: optNum(row.input_tokens),
    outputTokens: optNum(row.output_tokens),
    cachedTokens: optNum(row.cached_tokens),
    costMinor: optNum(row.cost_minor),
    currency: optText(row.currency),
    costBasis: optText(row.cost_basis),
    pricingRevision: optText(row.pricing_revision),
    recordedAt: String(row.recorded_at),
  };
}

function source(value: unknown): UsageSource {
  const row = record(value);
  const kind = row.kind;
  if (kind !== "connection" && kind !== "sumi") throw new Error(CHECK_FAILED);
  const recent = row.recent;
  if (!Array.isArray(recent)) throw new Error(CHECK_FAILED);
  return {
    kind,
    id: String(row.id),
    name: optText(row.name),
    model: optText(row.model),
    selected: row.selected === true,
    grant: row.grant === true,
    ...(row.budget ? { budget: budget(row.budget) } : {}),
    totals: totals(row.totals),
    recent: recent.map(fact),
  };
}

function wait(value: unknown): UsageWait {
  const row = record(value);
  return {
    personaId: String(row.persona_id),
    inputId: String(row.input_id),
    fundingKind: String(row.funding_kind),
    fundingId: String(row.funding_id),
    neededMinor: num(row.needed_minor),
    currency: String(row.currency),
    createdAt: String(row.created_at),
  };
}

export function createUsageAPI(
  fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
): UsageAPI {
  async function request(
    path: string,
    signal: AbortSignal,
    method = "GET",
    body?: unknown,
  ): Promise<unknown> {
    let response: Response;
    try {
      const csrfToken =
        method === "GET"
          ? undefined
          : await fetchCSRFToken({ fetcher, signal });
      signal.throwIfAborted();
      response = await fetcher(`/api/usage${path}`, {
        method,
        credentials: "include",
        cache: "no-store",
        headers: {
          Accept: "application/json",
          ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {}),
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.any([signal, AbortSignal.timeout(15_000)]),
      });
    } catch {
      throw new Error("通信できませんでした。接続状態を再確認してください。");
    }
    if (!response.ok) {
      let message = "利用状況の変更を確認できませんでした。";
      try {
        const failure = record(await response.json());
        if (typeof failure.error === "object" && failure.error !== null) {
          const detail = record(failure.error);
          if (typeof detail.message === "string" && detail.message) {
            message = detail.message;
          }
        }
      } catch {
        /* HTTP failure remains authoritative. */
      }
      throw new Error(message);
    }
    if (response.status === 204) return null;
    try {
      return await response.json();
    } catch {
      throw new Error("利用状況の応答を確認できませんでした。");
    }
  }

  function fundingPath(kind: UsageFundingKind, id: string): string {
    return `/funding/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`;
  }

  return {
    overview: async (signal) => {
      const row = record(await request("", signal));
      const sources = row.sources;
      const waits = row.waits;
      if (!Array.isArray(sources) || !Array.isArray(waits)) {
        throw new Error(CHECK_FAILED);
      }
      return { sources: sources.map(source), waits: waits.map(wait) };
    },
    setBudget: async (kind, id, input, signal) => {
      const row = record(
        await request(`${fundingPath(kind, id)}/budget`, signal, "PUT", {
          limit_minor: input.limitMinor,
          currency: input.currency,
          rate_input_per_mtok: input.rateInputPerMTok,
          rate_output_per_mtok: input.rateOutputPerMTok,
          ...(input.rateCachedPerMTok === undefined
            ? {}
            : { rate_cached_per_mtok: input.rateCachedPerMTok }),
          pricing_revision: input.pricingRevision,
        }),
      );
      return budget(row.budget);
    },
    clearBudget: async (kind, id, signal) => {
      await request(`${fundingPath(kind, id)}/budget`, signal, "DELETE");
    },
  };
}
