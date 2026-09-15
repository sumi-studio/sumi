import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { BudgetWaitError } from "../src/usage.ts";

/**
 * Usage accounting on the fake state: admission reserves priced estimate
 * spend before the call, one fact lands per logical call, a configured
 * budget denies further spend and its increase resumes parked work.
 * Rates here are labelled fixture rates, not provider prices.
 */

const PERSONA = "01930e00-0000-7000-8000-0000000000d1";
const HUMAN = "01930e00-0000-7000-8000-0000000000e1";

class Scripted implements ModelProvider {
  readonly name = "scripted";
  requests = 0;
  usage: Record<string, unknown> | null = {
    prompt_tokens: 100,
    completion_tokens: 20,
  };
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests++;
    yield { type: "text", delta: `ok ${req.messages.at(-1)?.content ?? ""}` };
    yield { type: "done", usage: this.usage ?? {} };
  }
}

/** A provider whose stream dies before a usage report resolves. */
class Dying implements ModelProvider {
  readonly name = "dying";
  requests = 0;
  async *stream(): AsyncIterable<ModelEvent> {
    this.requests++;
    yield { type: "text", delta: "partial" };
    throw new Error("connection lost");
  }
}

async function metered(state: FakeState, provider: ModelProvider) {
  state.addPersona(PERSONA, "test", HUMAN);
  const lease = await state.acquireWriter(PERSONA, "test-writer", 60_000);
  const p = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback: provider,
    timeoutMs: 5_000,
  });
  return { p, generation: lease.generation };
}

const REQ = (gen: number): ModelRequest => ({
  personaId: PERSONA,
  turnId: "t-1",
  generation: gen,
  phase: "turn",
  inputId: "in-1",
  round: 0,
  messages: [{ role: "user", content: "hi" }],
  tools: [],
});

// 1 unit per input token, 2 per output token — labelled fixture rates.
const RATES = {
  rate_input_per_mtok: 1_000_000,
  rate_output_per_mtok: 2_000_000,
  pricing_revision: "fixture-rates-v1",
};

test("usage: a metered call admits before streaming and records one fact", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  const { p, generation } = await metered(state, provider);

  const out: ModelEvent[] = [];
  for await (const ev of p.stream(REQ(generation))) out.push(ev);
  assert.equal(provider.requests, 1);

  const facts = await state.listUsageFacts(PERSONA);
  assert.equal(facts.length, 1);
  const f = facts[0]!;
  assert.equal(f.kind, "model_call");
  assert.equal(f.phase, "turn");
  assert.equal(f.funding.kind, "operator");
  assert.equal(f.funding.id, "env");
  assert.equal(f.status, "reported");
  assert.equal(f.input_tokens, 100);
  assert.equal(f.output_tokens, 20);
  // The held reservation settled when its fact landed.
  assert.equal(
    [...state.usageReservations.values()][0]!.status,
    "settled",
  );
});

test("usage: a call whose usage never resolves records 'unknown', never zero", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  provider.usage = null; // stream ends without a usage report
  const { p, generation } = await metered(state, provider);
  for await (const _ of p.stream(REQ(generation))) {
    // drain
  }
  const facts = await state.listUsageFacts(PERSONA);
  assert.equal(facts.length, 1);
  assert.equal(facts[0]!.status, "unknown");
  assert.equal(facts[0]!.input_tokens, null);
  assert.equal(facts[0]!.cost_minor, null);
});

test("usage: a stream that dies mid-call still records the attempt", async () => {
  const state = new FakeState();
  const provider = new Dying();
  const { p, generation } = await metered(state, provider);
  await assert.rejects(async () => {
    for await (const _ of p.stream(REQ(generation))) {
      // drain
    }
  }, /connection lost/);
  const facts = await state.listUsageFacts(PERSONA);
  assert.equal(facts.length, 1);
  assert.equal(facts[0]!.status, "unknown");
});

test("usage: a denied budget admits nothing and sends no provider request", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  const { p, generation } = await metered(state, provider);
  // Cap below the priced estimate of even this tiny request.
  state.setUsageBudget("operator", "env", {
    limit_minor: 1,
    currency: "USD",
    ...RATES,
  });
  let err: unknown;
  try {
    for await (const _ of p.stream(REQ(generation))) {
      // drain
    }
  } catch (e) {
    err = e;
  }
  assert.ok(err instanceof BudgetWaitError, `expected BudgetWaitError, got ${err}`);
  assert.equal(provider.requests, 0, "no provider request on denial");
  assert.equal(err.wait.funding.id, "env");
  assert.ok(err.wait.needed_minor > err.wait.limit_minor);
  // Nothing was reserved and nothing was recorded.
  assert.equal(state.usageReservations.size, 0);
  assert.equal((await state.listUsageFacts(PERSONA)).length, 0);
});

test("usage: recorded facts price against the configured rate card", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  const { p, generation } = await metered(state, provider);
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "USD",
    ...RATES,
  });
  for await (const _ of p.stream(REQ(generation))) {
    // drain
  }
  const fact = (await state.listUsageFacts(PERSONA))[0]!;
  // 100 input * 1 + 20 output * 2 = 140 units.
  assert.equal(fact.cost_minor, 140);
  assert.equal(fact.currency, "USD");
  assert.equal(fact.cost_basis, "configured_rates");
  assert.equal(fact.pricing_revision, "fixture-rates-v1");
});

function secretaryCfg(
  state: FakeState,
  provider: ModelProvider,
): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: "usage-test",
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 5_000,
    pollIntervalMs: 1,
    scheduleEveryMs: 60_000,
    idgen: () => crypto.randomUUID(),
  };
}

test("usage: a budget-denied turn parks the input; a raise resumes it", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "test", HUMAN);
  const scripted = new Scripted();
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback: scripted,
    timeoutMs: 5_000,
  });
  state.setUsageBudget("operator", "env", {
    limit_minor: 1,
    currency: "USD",
    ...RATES,
  });
  const s = new Secretary(secretaryCfg(state, provider));
  await s.start();
  state.addInput(PERSONA, "in-park", "hello");
  assert.equal(await s.step(), "turn");
  assert.equal(scripted.requests, 0, "denied admission sent no request");
  const input = state.inputs.find((i) => i.input_id === "in-park")!;
  assert.equal(input.status, "waiting");
  assert.equal(state.budgetWaits.size, 1);
  // Awaiting is not an attempt: the input did not fail.
  assert.equal(state.outboxEntries.at(-1)?.kind, "budget_wait");

  // Raising the cap resumes the parked input; the next step runs it.
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "USD",
    ...RATES,
  });
  assert.equal(input.status, "queued");
  assert.equal(state.budgetWaits.size, 0);
  assert.equal(await s.step(), "turn");
  assert.equal(scripted.requests, 1);
  assert.equal(input.status, "done");
  const facts = await state.listUsageFacts(PERSONA);
  assert.equal(facts.length, 1);
  assert.equal(facts[0]!.status, "reported");
  await s.stop();
});

test("usage: re-delivery of a fact is idempotent; a second call adds a second fact", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  const { p, generation } = await metered(state, provider);
  for await (const _ of p.stream(REQ(generation))) {
    // drain — the call records fact f1
  }
  const fact = (await state.listUsageFacts(PERSONA))[0]!;
  // Redeliver the identical record — replays, not duplicates.
  const replay = await state.recordUsage(PERSONA, {
    factId: fact.fact_id,
    kind: fact.kind,
    phase: fact.phase,
    turnId: fact.turn_id,
    inputId: fact.input_id,
    round: fact.round,
    funding: fact.funding,
    status: "reported",
    inputTokens: 100,
    outputTokens: 20,
    quantities: {},
  });
  assert.equal(replay.created, false);
  // A second real call is genuinely additional spend.
  for await (const _ of p.stream({ ...REQ(generation), turnId: "t-2" })) {
    // drain
  }
  assert.equal((await state.listUsageFacts(PERSONA)).length, 2);
});

// --- durable uncertain spend, admission pricing snapshot, wire-bound ----

import { reportedTokens } from "../src/usage.ts";
import { ModelError } from "../src/provider.ts";

test("usage: token normalization keeps categories non-overlapping per protocol", () => {
  // Chat Completions: prompt_tokens INCLUDES the cached subset.
  assert.deepEqual(
    reportedTokens({
      prompt_tokens: 100,
      completion_tokens: 20,
      prompt_tokens_details: { cached_tokens: 30 },
    }),
    { input: 70, output: 20, cached: 30 },
  );
  // Responses: same convention under input_tokens_details.
  assert.deepEqual(
    reportedTokens({
      input_tokens: 100,
      output_tokens: 20,
      input_tokens_details: { cached_tokens: 30 },
    }),
    { input: 70, output: 20, cached: 30 },
  );
  // Anthropic: input_tokens EXCLUDES both cache categories — cache-read is
  // the cached bucket; cache-write folds into input at the input rate.
  assert.deepEqual(
    reportedTokens({
      input_tokens: 50,
      output_tokens: 20,
      cache_read_input_tokens: 30,
      cache_creation_input_tokens: 10,
    }),
    { input: 60, output: 20, cached: 30 },
  );
  // A bare report keeps categories the provider did not send as null.
  assert.deepEqual(reportedTokens({ prompt_tokens: 10 }), {
    input: 10,
    output: null,
    cached: null,
  });
  // No recognizable usage at all: nothing is reported.
  assert.deepEqual(reportedTokens({}), {
    input: null,
    output: null,
    cached: null,
  });
});

test("usage: 'not_sent' releases the hold — a request that never left owes nothing", async () => {
  const state = new FakeState();
  const { generation } = await metered(state, new Scripted());
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000,
    currency: "USD",
    ...RATES,
  });
  // An 'unavailable' error is the provider's assertion that no request
  // was produced — bytes never left.
  const failing: ModelProvider = {
    name: "failing",
    stream: () => {
      throw new ModelError("endpoint refused", {
        retryable: true,
        unavailable: true,
      });
    },
  };
  const fp = new SelectedModelProvider({
    state, persona: PERSONA, fallback: failing, timeoutMs: 5_000,
  });
  await assert.rejects(async () => {
    for await (const _ of fp.stream(REQ(generation))) {
      // drain
    }
  }, /endpoint refused/);
  const fact = (await state.listUsageFacts(PERSONA))[0]!;
  assert.equal(fact.status, "not_sent");
  assert.equal(fact.cost_minor, null);
  assert.equal([...state.usageReservations.values()][0]!.status, "released");
});

test("usage: an early consumer return still records the fact", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  const { p, generation } = await metered(state, provider);
  const it = p.stream(REQ(generation))[Symbol.asyncIterator]();
  await it.next(); // consume one event, then abandon the stream
  await it.return?.();
  const facts = await state.listUsageFacts(PERSONA);
  assert.equal(facts.length, 1);
});

test("usage: an unknown fact under a budget keeps the admission estimate as uncertain spend", async () => {
  const state = new FakeState();
  const provider = new Scripted();
  provider.usage = null; // no usage report resolves
  const { p, generation } = await metered(state, provider);
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "USD",
    ...RATES,
  });
  for await (const _ of p.stream(REQ(generation))) {
    // drain
  }
  const res = [...state.usageReservations.values()][0]!;
  const fact = (await state.listUsageFacts(PERSONA))[0]!;
  assert.equal(fact.status, "unknown");
  // The reserved estimate stays spent — 'admission_estimate', not zero.
  assert.equal(fact.cost_minor, res.reserved_minor);
  assert.ok(fact.cost_minor! > 0);
  assert.equal(fact.cost_basis, "admission_estimate");

  // A late report for the same fact id upgrades it to the priced actual.
  const late = await state.recordUsage(PERSONA, {
    factId: fact.fact_id,
    kind: fact.kind,
    phase: fact.phase,
    turnId: fact.turn_id,
    inputId: fact.input_id,
    round: fact.round,
    funding: fact.funding,
    status: "reported",
    inputTokens: 100,
    outputTokens: 20,
    quantities: { prompt_tokens: 100, completion_tokens: 20 },
  });
  assert.equal(late.created, false);
  assert.equal(late.fact.status, "reported");
  // 100 input * 1 + 20 output * 2 = 140 under the snapshot card.
  assert.equal(late.fact.cost_minor, 140);
  assert.equal(late.fact.cost_basis, "configured_rates");
});

test("usage: a lost record reconciles to 'unrecorded', keeping the estimate", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "test", HUMAN);
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "USD",
    ...RATES,
  });
  const lease = await state.acquireWriter(PERSONA, "w-1", 60_000);
  // Admit, then the writer dies before any record lands.
  const admitted = await state.admitUsage(PERSONA, lease.generation, {
    factId: "f-lost",
    kind: "model_call",
    phase: "turn",
    turnId: "t-1",
    funding: { kind: "operator", id: "env" },
    estimate: { input_tokens: 100, output_tokens_bound: 50 },
  });
  assert.equal(admitted.admitted, true);
  // The writer dies: its lease expires and a new generation recovers the
  // orphaned hold into an inspectable fact.
  await state.releaseWriter(PERSONA, "w-1", lease.generation);
  const lease2 = await state.acquireWriter(PERSONA, "w-2", 60_000);
  await state.recover(PERSONA, lease2.generation);
  const fact = (await state.listUsageFacts(PERSONA))[0]!;
  assert.equal(fact.status, "unrecorded");
  assert.equal(fact.cost_basis, "admission_estimate");
  assert.ok(fact.cost_minor! > 0);
  assert.equal([...state.usageReservations.values()][0]!.status, "settled");
  // The unrecorded fact still accepts the late actual report.
  const late = await state.recordUsage(PERSONA, {
    factId: "f-lost",
    kind: "model_call",
    phase: "turn",
    turnId: "t-1",
    funding: { kind: "operator", id: "env" },
    status: "reported",
    inputTokens: 100,
    outputTokens: 20,
    quantities: {},
  });
  assert.equal(late.fact.status, "reported");
  assert.equal(late.fact.cost_minor, 140);
});

test("usage: a mid-call rate/currency change cannot rewrite an admitted call's price", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "test", HUMAN);
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "USD",
    ...RATES,
  });
  // Interleave: admit first, then edit the card mid-call, then record —
  // the admitted call keeps its snapshot; a later call prices under v2.
  const lease = await state.acquireWriter(PERSONA, "w-mid", 60_000);
  await state.admitUsage(PERSONA, lease.generation, {
    factId: "f-mid",
    kind: "model_call",
    phase: "turn",
    turnId: "t-mid",
    funding: { kind: "operator", id: "env" },
    estimate: { input_tokens: 100, output_tokens_bound: 20 },
  });
  // Mid-call: the human switches the card to JPY — the admitted call
  // keeps its USD snapshot; a LATER call prices under JPY.
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "JPY",
    rate_input_per_mtok: 10_000_000,
    rate_output_per_mtok: 20_000_000,
    pricing_revision: "fixture-rates-v2",
  });
  await state.recordUsage(PERSONA, {
    factId: "f-mid",
    kind: "model_call",
    phase: "turn",
    turnId: "t-mid",
    funding: { kind: "operator", id: "env" },
    status: "reported",
    inputTokens: 100,
    outputTokens: 20,
    quantities: {},
  });
  const mid = (await state.listUsageFacts(PERSONA))[0]!;
  assert.equal(mid.currency, "USD");
  assert.equal(mid.cost_minor, 140); // 100*1 + 20*2 under the snapshot

  // A call admitted under the JPY card prices in JPY; the two currencies
  // never sum into one integer.
  await state.admitUsage(PERSONA, lease.generation, {
    factId: "f-jpy",
    kind: "model_call",
    phase: "turn",
    turnId: "t-mid",
    funding: { kind: "operator", id: "env" },
    estimate: { input_tokens: 100, output_tokens_bound: 20 },
  });
  await state.recordUsage(PERSONA, {
    factId: "f-jpy",
    kind: "model_call",
    phase: "turn",
    turnId: "t-mid",
    funding: { kind: "operator", id: "env" },
    status: "reported",
    inputTokens: 100,
    outputTokens: 20,
    quantities: {},
  });
  const facts = await state.listUsageFacts(PERSONA);
  const jpy = facts.find((f) => f.fact_id === "f-jpy")!;
  assert.equal(jpy.currency, "JPY");
  assert.equal(jpy.cost_minor, 1400); // 100*10 + 20*20 under v2
  assert.equal(mid.currency, "USD");
});

test("usage: the estimate carries the provider's real wire bound", async () => {
  const state = new FakeState();
  const bounded: ModelProvider = {
    name: "bounded",
    outputBound: () => 42,
    async *stream(): AsyncIterable<ModelEvent> {
      yield { type: "done", usage: { prompt_tokens: 10, completion_tokens: 5 } };
    },
  };
  const { generation } = await metered(state, new Scripted());
  state.setUsageBudget("operator", "env", {
    limit_minor: 1_000_000,
    currency: "USD",
    ...RATES,
  });
  const bp = new SelectedModelProvider({
    state, persona: PERSONA, fallback: bounded, timeoutMs: 5_000,
  });
  for await (const _ of bp.stream(REQ(generation))) {
    // drain
  }
  const res = [...state.usageReservations.values()][0]!;
  assert.equal(res.bounded, true);
  assert.equal(res.est_output_bound, 42);
  // Reserved = est_input + 42 * output rate, not a fabricated generic cap.
  assert.ok(res.reserved_minor >= 42 * 2);
});
