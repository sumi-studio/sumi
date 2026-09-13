import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { assemble, Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { type StateClient, StateError } from "../src/state-client.ts";
import type { Event } from "../src/types.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000001";

/** Emits exactly one scripted decision; counts how often it was consulted. */
class ScriptedProvider implements ModelProvider {
  readonly name = "scripted";
  consultations = 0;
  private readonly script: {
    text: string;
    calls?: { tool: string; request: Record<string, unknown> }[];
  };
  constructor(script: {
    text: string;
    calls?: { tool: string; request: Record<string, unknown> }[];
  }) {
    this.script = script;
  }
  async *stream(_req: ModelRequest): AsyncIterable<ModelEvent> {
    this.consultations++;
    yield { type: "text", delta: this.script.text };
    for (const [i, c] of (this.script.calls ?? []).entries()) {
      yield {
        type: "tool_call",
        call: { id: `call-${i}`, name: c.tool, arguments: c.request },
      };
    }
    yield { type: "done", usage: { scripted: true } };
  }
}

function cfg(
  state: StateClient,
  holder = "test-1",
  over: Partial<SecretaryConfig> = {},
): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: holder,
    state,
    provider: new MockProvider(),
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 20,
    pollIntervalMs: 1,
    scheduleEveryMs: 0, // dispatch every step in tests
    idgen: () => crypto.randomUUID(),
    ...over,
  };
}

test("turn commits: input→model→outbox durable", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-1", "hello there");

  const s = new Secretary(cfg(state));
  await s.start();
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");

  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1);
  assert.equal(outbox[0]!.kind, "turn_completed");
  const output = (outbox[0]!.payload as { output: { text: string } }).output;
  assert.match(output.text, /echo: hello there/);

  const evs = await state.events(PERSONA, 0);
  assert.deepEqual(
    evs.map((e) => e.kind),
    ["input_received", "assistant_message"],
  );
});

test("internal tool: journal.note applies atomically and is observable", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-2", '!journal.note {"text":"likes green tea"}');

  const s = new Secretary(cfg(state));
  await s.start();
  await s.step();

  const evs = await state.events(PERSONA, 0);
  const note = evs.find((e) => e.kind === "note");
  assert.equal((note!.payload as { text: string }).text, "likes green tea");
  assert.ok(evs.some((e) => e.kind === "tool_result"));
});

test("schedule.set fires a durable wake input on dispatch", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const past = new Date(Date.now() - 1000).toISOString();
  state.addInput(
    PERSONA,
    "in-3",
    `!schedule.set {"wake_at":"${past}","payload":{"text":"time to follow up"}}`,
  );

  const s = new Secretary(cfg(state));
  await s.start();
  await s.step(); // turn 1: model calls schedule.set (atomic inside claim)
  await s.step(); // dispatch fires the due schedule → wake input; turn 2 consumes it

  const evs = await state.events(PERSONA, 0);
  const wake = evs.find(
    (e) =>
      e.kind === "input_received" &&
      (e.payload as { actor_kind: string }).actor_kind === "schedule",
  );
  assert.ok(wake, "expected a scheduled wake input event");
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 2);
});

test("crash mid-turn: next generation recovers and completes exactly once", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-4", '!journal.note {"text":"x"}');

  // Simulate a dead writer: lease acquired, turn claimed, never committed.
  const dead = new Secretary(cfg(state, "dead-holder"));
  await dead.start();
  const deadGen = dead.generation!;
  const { turn, input } = await state.loadTurn(PERSONA, deadGen, "t-dead", 10);
  assert.ok(turn && input);
  // Force-expire the lease so a new writer can take over.
  const held = state.leases.get(PERSONA)!;
  state.leases.set(PERSONA, {
    ...held,
    expires_at: new Date(Date.now() - 1).toISOString(),
  });

  const alive = new Secretary(cfg(state, "alive-holder"));
  await alive.start();
  assert.equal(alive.generation, deadGen + 1);
  const rec = await state.personaState(PERSONA);
  assert.equal(rec.lease!.holder_id, "alive-holder");
  assert.equal(await alive.step(), "turn"); // requeued input retried
  assert.equal(await alive.step(), "idle");

  const evs = await state.events(PERSONA, 0);
  assert.equal(
    evs.filter((e) => e.kind === "note").length,
    1,
    "note written exactly once",
  );
  const done = state.inputs.find((i) => i.input_id === "in-4")!;
  assert.equal(done.status, "done");
  assert.equal(state.turns.get("t-dead")!.status, "interrupted");
});

test("fencing: second writer cannot acquire; stale generation rejected", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const a = new Secretary(cfg(state, "a"));
  await a.start();
  const b = new Secretary(cfg(state, "b"));
  await assert.rejects(b.start(), /held by another/);
  // Stale generation calls are fenced off.
  await assert.rejects(
    state.loadTurn(PERSONA, a.generation! + 99, "t-x", 10),
    /fenced/,
  );
});

test("replay after lost response: committing the same input twice yields one result", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-5", "hi");
  const s = new Secretary(cfg(state));
  await s.start();
  await s.step();
  // The HTTP caller lost the response; re-running finds input done, no new turn.
  assert.equal(await s.step(), "idle");
  const evs = await state.events(PERSONA, 0);
  assert.equal(evs.filter((e) => e.kind === "input_received").length, 1);
});

test("transient claim failure leaves the turn running; retry recovers (F4)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-6", '!journal.note {"text":"must persist"}');
  let claims = 0;
  const flaky: StateClient = Object.create(state, {
    claimOperation: {
      value: async (
        p: string,
        g: number,
        op: Parameters<StateClient["claimOperation"]>[2],
      ) => {
        if (++claims === 1)
          throw new StateError(503, "simulated transient pool exhaustion");
        return state.claimOperation(p, g, op);
      },
    },
  });
  const s = new Secretary(cfg(flaky));
  await s.start();
  // The transient error propagates — it must NOT be committed as a fake
  // tool result. The turn stays running; the input stays claimed.
  await assert.rejects(s.step(), /transient pool exhaustion/);
  assert.equal((await state.personaState(PERSONA)).running_turn !== null, true);
  assert.equal(state.inputs.find((i) => i.input_id === "in-6")!.status, "claimed");

  // Next step replays the same running turn (lost-response path) and
  // completes it — the note lands exactly once, no fabricated failure.
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");
  const evs = await state.events(PERSONA, 0);
  assert.equal(evs.filter((e) => e.kind === "note").length, 1);
  assert.equal(
    evs.filter((e) => e.kind === "tool_result" && e.payload.error).length,
    0,
    "no fabricated tool_result error may be committed",
  );
  assert.equal(state.inputs.find((i) => i.input_id === "in-6")!.status, "done");
});

test("durable plan: crash after an effect → retry continues the recorded plan (F1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-7", "remember things");
  const planA = new ScriptedProvider({
    text: "reply-A",
    calls: [
      { tool: "journal.note", request: { text: "note-A0" } },
      { tool: "journal.note", request: { text: "note-A1" } },
    ],
  });
  // Attempt 1 commits the position-0 effect, then dies before commitTurn.
  let claims = 0;
  const crashy: StateClient = Object.create(state, {
    claimOperation: {
      value: async (
        p: string,
        g: number,
        op: Parameters<StateClient["claimOperation"]>[2],
      ) => {
        const r = await state.claimOperation(p, g, op);
        if (++claims === 1) throw new Error("simulated hard exit");
        return r;
      },
    },
  });
  const s1 = new Secretary(cfg(crashy, "h", { provider: planA }));
  await s1.start();
  await assert.rejects(s1.step(), /hard exit/);
  assert.equal(
    (await state.events(PERSONA, 0)).filter(
      (e) => e.kind === "note" && e.payload.text === "note-A0",
    ).length,
    1,
    "the position-0 effect committed before the crash",
  );

  // Attempt 2 carries a provider that WOULD decide differently. The
  // recorded plan wins and the model is never consulted.
  const planB = new ScriptedProvider({
    text: "reply-B",
    calls: [{ tool: "journal.note", request: { text: "note-B0" } }],
  });
  const s2 = new Secretary(cfg(state, "h", { provider: planB }));
  await s2.start(); // same holder: re-acquires, bumps generation, recovers
  assert.equal(await s2.step(), "turn");
  assert.equal(await s2.step(), "idle");

  assert.equal(planA.consultations, 1);
  assert.equal(planB.consultations, 0, "retried attempt must not re-plan");
  const evs = await state.events(PERSONA, 0);
  const texts = evs.filter((e) => e.kind === "note").map((e) => e.payload.text);
  assert.deepEqual(texts.sort(), ["note-A0", "note-A1"]);
  assert.ok(
    evs.some(
      (e) => e.kind === "tool_result" && e.payload.replayed === true,
    ),
    "position 0 replayed its stored receipt",
  );
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1);
  assert.equal(
    (outbox[0]!.payload as { output: { text: string } }).output.text,
    "reply-A",
    "the recorded plan's text is committed, not the retry's",
  );
  assert.equal(state.inputs.find((i) => i.input_id === "in-7")!.status, "done");
});

test("savePlan: identical resend returns the stored plan; conflict is rejected", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-8", "hi");
  const s = new Secretary(cfg(state));
  await s.start();
  const gen = s.generation!;
  const { turn } = await state.loadTurn(PERSONA, gen, "t-1", 10);
  const req = {
    turnId: turn!.turn_id,
    text: "reply",
    calls: [{ tool: "journal.note", request: { text: "n" } }],
    usage: { in: 1, out: 2 },
  };
  const first = await state.savePlan(PERSONA, gen, req);
  assert.equal(first.created, true);
  assert.equal(first.plan.plan.text, "reply");
  // Lost-response resend: identical body → stored row, not a new write.
  const replay = await state.savePlan(PERSONA, gen, req);
  assert.equal(replay.created, false);
  assert.equal(replay.plan.turn_id, "t-1");
  // Key order / equivalent JSON must not false-conflict.
  const reordered = {
    ...req,
    calls: [{ request: { text: "n" }, tool: "journal.note" }],
  };
  assert.equal((await state.savePlan(PERSONA, gen, reordered)).created, false);
  // A different decision for the same input is a contract violation.
  await assert.rejects(
    state.savePlan(PERSONA, gen, { ...req, text: "different" }),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );
});

test("claims require the recorded plan and must match its positions", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-9", "hi");
  const s = new Secretary(cfg(state));
  await s.start();
  const gen = s.generation!;
  const { turn } = await state.loadTurn(PERSONA, gen, "t-1", 10);
  const claim = (over: Partial<Parameters<StateClient["claimOperation"]>[2]>) =>
    state.claimOperation(PERSONA, gen, {
      operationId: "op-x",
      turnId: turn!.turn_id,
      tool: "journal.note",
      callIndex: 0,
      request: { text: "n" },
      ...over,
    });
  // No plan → no claims.
  await assert.rejects(
    claim({}),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );
  await state.savePlan(PERSONA, gen, {
    turnId: turn!.turn_id,
    text: "reply",
    calls: [{ tool: "journal.note", request: { text: "n" } }],
    usage: {},
  });
  // Off-plan request, off-plan tool, and out-of-range index all conflict.
  for (const bad of [
    { request: { text: "different" } },
    { tool: "schedule.set", request: { text: "n" } },
    { callIndex: 1 },
  ]) {
    await assert.rejects(
      claim(bad),
      (e: unknown) => e instanceof StateError && e.status === 409,
    );
  }
  // On-plan claim executes and replays.
  assert.equal((await claim({})).fresh, true);
  const replay = await claim({ operationId: "op-y" });
  assert.equal(replay.fresh, false);
  assert.equal(replay.operation.operation_id, "op-x");
});

test("server-derived effect identity: a new operation_id on retry replays the receipt", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-10", "hi");
  const s = new Secretary(cfg(state));
  await s.start();
  const gen = s.generation!;
  const { turn } = await state.loadTurn(PERSONA, gen, "t-old", 10);
  await state.savePlan(PERSONA, gen, {
    turnId: turn!.turn_id,
    text: "reply",
    calls: [{ tool: "journal.note", request: { text: "committed" } }],
    usage: {},
  });
  await state.claimOperation(PERSONA, gen, {
    operationId: "t-old:op:0",
    turnId: "t-old",
    tool: "journal.note",
    callIndex: 0,
    request: { text: "committed" },
  });
  await state.recover(PERSONA, gen); // interrupt t-old, requeue in-10

  // A later attempt under a new turn_id/operation_id hits the same
  // server-derived key (input_id + call_index): replay, never a second effect.
  const { turn: t2 } = await state.loadTurn(PERSONA, gen, "t-new", 10);
  const replay = await state.claimOperation(PERSONA, gen, {
    operationId: "t-new:op:0",
    turnId: t2!.turn_id,
    tool: "journal.note",
    callIndex: 0,
    request: { text: "committed" },
  });
  assert.equal(replay.fresh, false);
  assert.equal(replay.operation.operation_id, "t-old:op:0");
  assert.equal(
    (await state.events(PERSONA, 0)).filter((e) => e.kind === "note").length,
    1,
  );
});

test("zero-call plan round-trips; a crash before commit replays it without the model", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-11", "just text");
  const planC = new ScriptedProvider({ text: "reply-C", calls: [] });
  let commits = 0;
  const crashy: StateClient = Object.create(state, {
    commitTurn: {
      value: async (
        p: string,
        t: string,
        g: number,
        req: Parameters<StateClient["commitTurn"]>[3],
      ) => {
        if (++commits === 1) throw new Error("simulated hard exit");
        return state.commitTurn(p, t, g, req);
      },
    },
  });
  const s1 = new Secretary(cfg(crashy, "h", { provider: planC }));
  await s1.start();
  await assert.rejects(s1.step(), /hard exit/);
  assert.equal(state.plans.size, 1, "the zero-call decision was persisted");
  assert.equal([...state.plans.values()][0]!.plan.calls.length, 0);

  const planD = new ScriptedProvider({ text: "reply-D" });
  const s2 = new Secretary(cfg(state, "h", { provider: planD }));
  await s2.start();
  assert.equal(await s2.step(), "turn");
  assert.equal(planD.consultations, 0, "zero-call plan replays unsupervised");
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(
    (outbox[0]!.payload as { output: { text: string } }).output.text,
    "reply-C",
  );
});

test("conflicting stored plan on save fails the turn loudly, non-retryable", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-12", "hi");
  const s = new Secretary(cfg(state, "h", { provider: new ScriptedProvider({ text: "B" }) }));
  await s.start();
  const gen = s.generation!;
  // A prior attempt recorded a different decision, then died.
  const { turn } = await state.loadTurn(PERSONA, gen, "t-old", 10);
  await state.savePlan(PERSONA, gen, {
    turnId: turn!.turn_id,
    text: "A",
    calls: [],
    usage: {},
  });
  await state.recover(PERSONA, gen);
  // Corrupt/misbehaving client path: loadTurn reports no plan (so the core
  // consults the model) while the store still holds one — savePlan 409s.
  const blind: StateClient = Object.create(state, {
    loadTurn: {
      value: async (
        ...args: Parameters<StateClient["loadTurn"]>
      ): ReturnType<StateClient["loadTurn"]> => {
        const r = await state.loadTurn(...args);
        return { ...r, plan: null };
      },
    },
  });
  const s2 = new Secretary(cfg(blind, "h", { provider: new ScriptedProvider({ text: "B" }) }));
  await s2.start();
  assert.equal(await s2.step(), "turn");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-12" && t.status === "failed",
  );
  assert.ok(failed, "turn must fail loudly on the plan conflict");
  assert.match(failed!.error ?? "", /conflicting plan|diverged retry/);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-12")!.status,
    "done",
    "non-retryable — no poison loop",
  );
});

test("transient savePlan failure leaves the turn running; nothing is committed", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-13", '!journal.note {"text":"x"}');
  let saves = 0;
  const flaky: StateClient = Object.create(state, {
    savePlan: {
      value: async (
        ...args: Parameters<StateClient["savePlan"]>
      ): ReturnType<StateClient["savePlan"]> => {
        if (++saves === 1)
          throw new StateError(503, "simulated transient pool exhaustion");
        return state.savePlan(...args);
      },
    },
  });
  const s = new Secretary(cfg(flaky));
  await s.start();
  await assert.rejects(s.step(), /transient pool exhaustion/);
  assert.equal((await state.personaState(PERSONA)).running_turn !== null, true);
  assert.equal(state.plans.size, 0, "no decision was persisted");
  assert.equal(state.inputs.find((i) => i.input_id === "in-13")!.status, "claimed");
  // Recovery replays the running turn, re-plans (nothing committed), succeeds.
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");
  assert.equal(
    (await state.events(PERSONA, 0)).filter((e) => e.kind === "note").length,
    1,
  );
});

test("a store replaying a receipt for a different request is still caught client-side", async () => {
  // Defense in depth below the plan binding: if a store ever returned a
  // stored op whose request differs from the claimed plan position, the
  // core fails loudly instead of committing a fabricated result.
  const inner = new FakeState();
  inner.addPersona(PERSONA);
  inner.addInput(PERSONA, "in-14", "hi");
  const s0 = new Secretary(cfg(inner, "h"));
  await s0.start();
  const gen = s0.generation!;
  const { turn } = await inner.loadTurn(PERSONA, gen, "t-old", 10);
  await inner.savePlan(PERSONA, gen, {
    turnId: turn!.turn_id,
    text: "reply",
    calls: [{ tool: "journal.note", request: { text: "A" } }],
    usage: {},
  });
  await inner.claimOperation(PERSONA, gen, {
    operationId: "t-old:op:0",
    turnId: "t-old",
    tool: "journal.note",
    callIndex: 0,
    request: { text: "A" },
  });
  await inner.recover(PERSONA, gen);
  const stored = [...inner.ops.values()][0]!;
  const lenient: StateClient = Object.create(inner, {
    claimOperation: {
      value: async () => ({
        operation: { ...stored, request: { text: "different" } },
        fresh: false,
      }),
    },
  });
  const s2 = new Secretary(cfg(lenient, "h"));
  await s2.start();
  assert.equal(await s2.step(), "turn");
  const failed = [...inner.turns.values()].find(
    (t) => t.input_id === "in-14" && t.turn_id !== "t-old",
  )!;
  assert.equal(failed.status, "failed");
  assert.match(failed.error ?? "", /diverged retry/);
});

test("assemble flattens tool results to assistant text (no orphaned tool role)", () => {
  const context: Event[] = [
    {
      persona_id: PERSONA,
      seq: 1,
      turn_id: "t1",
      kind: "tool_result",
      payload: {
        tool: "journal.note",
        call_id: "c0",
        response: { seq: 1, kind: "note" },
      },
      created_at: new Date().toISOString(),
    },
  ];
  const input = {
    persona_id: PERSONA,
    input_id: "in-9",
    kind: "message",
    payload: { text: "hi" },
    actor_kind: "human",
    actor_id: "t",
    source_surface: "test",
    thread_id: "",
    occurred_at: null,
    attention: "reply" as const,
    status: "queued" as const,
    claimed_generation: null,
    turn_id: null,
    created_at: new Date().toISOString(),
    done_at: null,
  };
  const messages = assemble(context, input);
  assert.equal(
    messages.filter((m) => m.role === "tool").length,
    0,
    "no orphaned role:tool messages",
  );
  const flat = messages.find((m) => m.content.includes("[tool journal.note]"));
  assert.equal(flat?.role, "assistant");
});

test("same-holder acquire bumps generation; release keeps monotonic fencing", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const l1 = await state.acquireWriter(PERSONA, "h", 30_000);
  // Go: the same holder may re-acquire an unexpired lease → generation+1.
  const l2 = await state.acquireWriter(PERSONA, "h", 30_000);
  assert.equal(l2.generation, l1.generation + 1);
  // Mutations under the superseded generation are fenced.
  await assert.rejects(state.loadTurn(PERSONA, l1.generation, "t-x", 10), /fenced/);
  // Release never recycles the generation (B1).
  await state.releaseWriter(PERSONA, "h", l2.generation);
  const l3 = await state.acquireWriter(PERSONA, "h", 30_000);
  assert.equal(l3.generation, l2.generation + 1);
});
