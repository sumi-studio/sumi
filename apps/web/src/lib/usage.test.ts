import { expect, it, vi } from "vitest";
import { createUsageAPI } from "./usage";

it("reads owned funding sources, keeps unknown usage distinct, and sends CSRF on budget writes", async () => {
  const calls: Array<[string, RequestInit | undefined]> = [];
  const fetcher = vi.fn(async (path: RequestInfo | URL, init?: RequestInit) => {
    calls.push([String(path), init]);
    if (path === "/auth/csrf")
      return Response.json({ csrf_token: "a".repeat(43) });
    if (init?.method === "DELETE") return new Response(null, { status: 204 });
    if (init?.method === "PUT")
      return Response.json({
        budget: {
          funding_kind: "connection",
          funding_id: "conn-1",
          limit_minor: 1000,
          currency: "USD",
          rate_input_per_mtok: 250,
          rate_output_per_mtok: 1000,
          rate_cached_per_mtok: null,
          pricing_revision: "settings-2026-09-15",
          updated_at: "2026-09-15T00:00:00Z",
          spent_minor: 300,
          held_minor: 0,
          remaining_minor: 700,
        },
      });
    return Response.json({
      sources: [
        {
          kind: "connection",
          id: "conn-1",
          name: "Personal",
          model: "custom-model",
          selected: true,
          totals: {
            calls: 2,
            unknown_calls: 1,
            unpriced_calls: 0,
            input_tokens: 10,
            output_tokens: 5,
            cached_tokens: 3,
            cost_minor: 300,
            currency: "USD",
          },
          recent: [
            {
              persona_id: "p1",
              fact_id: "f1",
              kind: "model_call",
              phase: "turn",
              funding: { kind: "connection", id: "conn-1" },
              status: "unknown",
              input_tokens: null,
              output_tokens: null,
              cached_tokens: null,
              quantities: {},
              cost_minor: null,
              recorded_at: "2026-09-15T00:00:00Z",
            },
          ],
        },
        {
          kind: "sumi",
          id: "grant-1",
          grant: true,
          selected: false,
          totals: {
            calls: 0,
            unknown_calls: 0,
            unpriced_calls: 0,
            input_tokens: 0,
            output_tokens: 0,
            cached_tokens: 0,
            cost_minor: 0,
          },
          recent: [],
        },
      ],
      waits: [
        {
          persona_id: "p1",
          input_id: "i1",
          turn_id: "t1",
          funding_kind: "connection",
          funding_id: "conn-1",
          needed_minor: 500,
          currency: "USD",
          created_at: "2026-09-15T00:00:00Z",
        },
      ],
    });
  });
  const client = createUsageAPI(fetcher);
  const signal = new AbortController().signal;
  const overview = await client.overview(signal);
  expect(overview.sources).toHaveLength(2);
  expect(overview.sources[0]?.grant).toBe(false);
  expect(overview.sources[1]?.grant).toBe(true);
  expect(overview.sources[0]?.recent[0]?.status).toBe("unknown");
  expect(overview.sources[0]?.recent[0]?.costMinor).toBeUndefined();
  expect(overview.waits[0]?.neededMinor).toBe(500);
  const saved = await client.setBudget(
    "connection",
    "conn-1",
    {
      limitMinor: 1000,
      currency: "USD",
      rateInputPerMTok: 250,
      rateOutputPerMTok: 1000,
      pricingRevision: "settings-2026-09-15",
    },
    signal,
  );
  expect(saved.remainingMinor).toBe(700);
  await client.clearBudget("connection", "conn-1", signal);
  const mutations = calls.filter(
    ([, init]) => init?.method && init.method !== "GET",
  );
  expect(mutations.map(([path]) => path)).toEqual([
    "/api/usage/funding/connection/conn-1/budget",
    "/api/usage/funding/connection/conn-1/budget",
  ]);
  for (const [, init] of mutations) {
    expect(new Headers(init?.headers).get("X-CSRF-Token")).toBe("a".repeat(43));
  }
  expect(JSON.parse(String(mutations[0]?.[1]?.body))).toEqual({
    limit_minor: 1000,
    currency: "USD",
    rate_input_per_mtok: 250,
    rate_output_per_mtok: 1000,
    pricing_revision: "settings-2026-09-15",
  });
});
