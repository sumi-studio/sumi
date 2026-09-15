// A model layer that cannot produce a request — needs_rebinding after a
// transfer, selection "none", a missing credential, a selection-lookup
// outage — pauses unfinished memory preparation instead of recording a
// verdict. No attempts or interruptions are spent; once a usable binding
// exists the same chunk claims and prepares normally.
import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import { runMemoryPreparation } from "../src/memory.ts";
import { ModelError, type ModelProvider } from "../src/provider.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { StateError } from "../src/state-client.ts";
import type { MemoryChunk } from "../src/types.ts";

const PERSONA = "01930e00-0000-7000-8000-0000000000b1";

function pushChunk(
  state: FakeState,
  fields: Partial<MemoryChunk> & {
    chunk_seq: number;
    layer: number;
    first_seq: number;
    last_seq: number;
  },
): void {
  state.memoryChunks.push({
    persona_id: PERSONA,
    sources: null,
    est_tokens: 10_000,
    status: "sealed",
    replacement: null,
    replacement_est_tokens: null,
    attempts: 0,
    interruptions: 0,
    last_error: null,
    claimed_generation: null,
    claimed_at: null,
    not_before: null,
    created_at: "2026-09-15T00:00:00.000Z",
    prepared_at: null,
    applied_at: null,
    ...fields,
  });
}

function seedJournal(state: FakeState, count: number): void {
  for (let i = 1; i <= count; i++) {
    state.eventLog.push({
      persona_id: PERSONA,
      seq: i,
      turn_id: "t",
      kind: i % 2 === 1 ? "input_received" : "assistant_message",
      payload:
        i % 2 === 1
          ? {
              input_id: `u${i}`,
              kind: "message",
              payload: { text: `said ${i}` },
              actor_kind: "human",
            }
          : { text: `answer ${i}` },
      created_at: `2026-09-15T00:0${i}:00.000Z`,
    });
  }
}

class EchoFallback implements ModelProvider {
  readonly name = "fallback";
  calls = 0;
  async *stream() {
    this.calls++;
    yield { type: "text" as const, delta: "condensed memory" };
    yield { type: "done" as const, usage: {} };
  }
}

const chunk = (state: FakeState, seq: number): MemoryChunk => {
  const c = state.memoryChunks.find((x) => x.chunk_seq === seq);
  assert.ok(c, `chunk ${seq} exists`);
  return c;
};

test("needs_rebinding pauses carried chunks; rebinding prepares them", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  seedJournal(state, 4);
  pushChunk(state, { chunk_seq: 1, layer: 1, first_seq: 1, last_seq: 2 });
  pushChunk(state, { chunk_seq: 3, layer: 1, first_seq: 3, last_seq: 4 });
  const gen = (await state.acquireWriter(PERSONA, "h", 30_000)).generation;

  // The ordinary post-transfer state: a carried intent not yet bound.
  state.setModelBinding(PERSONA, {
    selection: "needs_rebinding",
    intent: { kind: "api", model: "model-a" },
    reason: "carried model intent needs a destination selection",
  } as never);
  const fallback = new EchoFallback();
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback,
    timeoutMs: 5_000,
  });
  const deps = {
    personaId: PERSONA,
    generation: gen,
    state,
    provider,
    contextLimit: 5_000,
    system: "SYS",
    tools: [],
  };

  // Repeated ticks while unbound: nothing is claimed, no model is
  // consulted, no verdict or attempt is recorded.
  for (let i = 0; i < 3; i++) {
    await runMemoryPreparation(deps);
    assert.equal(fallback.calls, 0, "no model request was sent");
    for (const seq of [1, 3]) {
      const c = chunk(state, seq);
      assert.equal(c.status, "sealed");
      assert.equal(c.attempts, 0);
      assert.equal(c.interruptions, 0);
      assert.equal(c.last_error, null, "a pause records no verdict");
    }
  }

  // The human binds a usable selection: the same chunks proceed.
  state.setModelBinding(PERSONA, { selection: "unset" });
  await runMemoryPreparation(deps);
  assert.equal(fallback.calls, 1);
  const c1 = chunk(state, 1);
  assert.equal(c1.status, "prepared");
  assert.equal(c1.replacement, "condensed memory");
  await runMemoryPreparation(deps);
  assert.equal(chunk(state, 3).status, "prepared");
});

test("a binding that dies mid-call reshelves without spending attempts", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  seedJournal(state, 2);
  pushChunk(state, { chunk_seq: 1, layer: 1, first_seq: 1, last_seq: 2 });
  const gen = (await state.acquireWriter(PERSONA, "h", 30_000)).generation;

  // The probe sees a usable binding, then the selection lookup dies
  // before the stream resolves — the race the preflight cannot cover.
  const real = state.modelBinding.bind(state);
  let lookups = 0;
  state.modelBinding = (persona: string) => {
    lookups++;
    if (lookups === 1) return real(persona);
    return Promise.reject(new StateError(503, "state service restarting"));
  };
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback: new EchoFallback(),
    timeoutMs: 5_000,
  });

  await runMemoryPreparation({
    personaId: PERSONA,
    generation: gen,
    state,
    provider,
    contextLimit: 5_000,
    system: "SYS",
    tools: [],
  });
  const c = chunk(state, 1);
  assert.equal(c.status, "sealed", "unavailable claim returns to the shelf");
  assert.equal(c.attempts, 0, "no attempt spent on a call never evaluated");
  assert.equal(c.interruptions, 0, "not an interruption either");
  assert.ok(c.not_before, "paced so a persistent outage does not spin");
  assert.match(c.last_error ?? "", /selection lookup failed/);
  state.modelBinding = real;

  // Once the outage clears and pacing passes, the same chunk prepares.
  c.not_before = null;
  await runMemoryPreparation({
    personaId: PERSONA,
    generation: gen,
    state,
    provider,
    contextLimit: 5_000,
    system: "SYS",
    tools: [],
  });
  assert.equal(chunk(state, 1).status, "prepared");
});

test("an unbound selection leaves claimable chunks for later", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  seedJournal(state, 2);
  pushChunk(state, { chunk_seq: 1, layer: 1, first_seq: 1, last_seq: 2 });
  state.setModelBinding(PERSONA, { selection: "none" });
  const fallback = new EchoFallback();
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback,
    timeoutMs: 5_000,
  });
  const cfg: SecretaryConfig = {
    personaId: PERSONA,
    holderId: "binding-test",
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 5_000,
    pollIntervalMs: 1,
    scheduleEveryMs: 60_000,
    idgen: () => crypto.randomUUID(),
  };
  const s = new Secretary(cfg);
  await s.start();
  assert.equal(await s.step(), "idle");
  for (let i = 0; i < 2_000 && s.memoryBusy; i++) {
    await new Promise((r) => setTimeout(r, 2));
  }
  const c = chunk(state, 1);
  assert.equal(c.status, "sealed", "idle step must not burn the chunk");
  assert.equal(c.attempts, 0);
  assert.equal(fallback.calls, 0);

  // A usable selection: the next tick's preparation proceeds normally.
  state.setModelBinding(PERSONA, { selection: "unset" });
  assert.equal(await s.step(), "idle");
  for (let i = 0; i < 2_000 && s.memoryBusy; i++) {
    await new Promise((r) => setTimeout(r, 2));
  }
  assert.equal(chunk(state, 1).status, "prepared");
  assert.equal(fallback.calls, 1);
});

test("non-binding failures still record honest verdicts", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  seedJournal(state, 2);
  pushChunk(state, { chunk_seq: 1, layer: 1, first_seq: 1, last_seq: 2 });
  const gen = (await state.acquireWriter(PERSONA, "h", 30_000)).generation;

  // A provider error during the stream is a real preparation failure —
  // retryable within the attempt budget, unchanged by the pause path.
  const failing: ModelProvider = {
    name: "failing",
    stream: () => {
      const err = new ModelError("upstream 500", { retryable: true });
      return {
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.reject(err),
        }),
      };
    },
  };
  await runMemoryPreparation({
    personaId: PERSONA,
    generation: gen,
    state,
    provider: failing,
    contextLimit: 5_000,
    system: "SYS",
    tools: [],
  });
  const c = chunk(state, 1);
  assert.equal(c.status, "sealed");
  assert.equal(c.attempts, 1, "a real model failure spends an attempt");
  assert.match(c.last_error ?? "", /upstream 500/);
});
