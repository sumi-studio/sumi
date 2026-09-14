import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
} from "../src/provider.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { assemble, Secretary, type SecretaryConfig } from "../src/secretary.ts";
import {
  HttpStateClient,
  type StateClient,
  StateError,
} from "../src/state-client.ts";
import { toolSpecs } from "../src/tools.ts";
import type { Event } from "../src/types.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000001";

/** Emits one scripted decision per round; records which rounds were consulted. */
class ScriptedProvider implements ModelProvider {
  readonly name = "scripted";
  consultations: number[] = [];
  requests: ModelRequest[] = [];
  private readonly rounds: {
    text: string;
    calls?: { tool: string; request: Record<string, unknown> }[];
  }[];
  constructor(
    script:
      | { text: string; calls?: { tool: string; request: Record<string, unknown> }[] }
      | {
          rounds: {
            text: string;
            calls?: { tool: string; request: Record<string, unknown> }[];
          }[];
        },
  ) {
    this.rounds = "rounds" in script ? script.rounds : [script];
  }
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.consultations.push(req.round);
    this.requests.push(req);
    const r = this.rounds[req.round] ?? { text: "", calls: [] };
    yield { type: "text", delta: r.text };
    for (const [i, c] of (r.calls ?? []).entries()) {
      yield {
        type: "tool_call",
        call: { id: `call-${req.round}-${i}`, name: c.tool, arguments: c.request },
      };
    }
    yield { type: "done", usage: { scripted: true, round: req.round } };
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
  // Attempt 1 commits the position-0 effect, then dies before commitTurn —
  // only round 0 is recorded when the process goes away.
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

  // Attempt 2 carries a provider that WOULD decide differently at round 0.
  // The recorded round is replayed verbatim — the model is consulted only
  // for the still-unrecorded final round, fed by the committed receipts.
  const planB = new ScriptedProvider({
    rounds: [
      {
        text: "reply-B",
        calls: [{ tool: "journal.note", request: { text: "note-B0" } }],
      },
      { text: "reply-from-attempt-2" },
    ],
  });
  const s2 = new Secretary(cfg(state, "h", { provider: planB }));
  await s2.start(); // same holder: re-acquires, bumps generation, recovers
  assert.equal(await s2.step(), "turn");
  assert.equal(await s2.step(), "idle");

  assert.deepEqual(planA.consultations, [0]);
  assert.deepEqual(
    planB.consultations,
    [1],
    "retried attempt replays recorded round 0 and consults only round 1",
  );
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
    "reply-from-attempt-2",
    "the final round was never recorded — the retry's consultation answers it",
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
    round: 0,
    text: "reply",
    calls: [{ tool: "journal.note", request: { text: "n" } }],
    usage: { in: 1, out: 2 },
  };
  const first = await state.savePlan(PERSONA, gen, req);
  assert.equal(first.created, true);
  assert.equal(first.plan.plan[0]!.text, "reply");
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
    round: 0,
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
    round: 0,
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
  assert.equal([...state.plans.values()][0]!.plan[0]!.calls.length, 0);

  const planD = new ScriptedProvider({ text: "reply-D" });
  const s2 = new Secretary(cfg(state, "h", { provider: planD }));
  await s2.start();
  assert.equal(await s2.step(), "turn");
  assert.deepEqual(planD.consultations, [], "zero-call plan replays unsupervised");
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
    round: 0,
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
    round: 0,
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
    not_before: null,
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

test("assemble shows Messaging provenance a reply can be addressed to", () => {
  const place = "0190a8a0-0000-7000-8000-000000000001";
  const earlier: Event[] = [
    {
      persona_id: PERSONA,
      seq: 1,
      turn_id: "t1",
      kind: "input_received",
      payload: {
        text: "みんなへの周知",
        actor_kind: "human",
        actor_display: "Haru",
        source_surface: "messaging",
        thread_id: place,
        place_name: "general",
        place_kind: "channel",
        message_id: "m-1",
        attention: "observe",
      },
      created_at: new Date().toISOString(),
    },
  ];
  const input = {
    persona_id: PERSONA,
    input_id: "messaging:e-2",
    kind: "message",
    payload: {
      text: "見てくれる？",
      actor: { kind: "personality_agent", display_name: "Shiro [bot]" },
      place: { id: place, kind: "channel", name: "general" },
      message_id: "m-2",
    },
    actor_kind: "personality_agent",
    actor_id: "pa-2",
    source_surface: "messaging",
    thread_id: place,
    occurred_at: new Date().toISOString(),
    attention: "reply" as const,
    status: "queued" as const,
    claimed_generation: null,
    turn_id: null,
    created_at: new Date().toISOString(),
    done_at: null,
    not_before: null,
  };
  const messages = assemble(earlier, input);
  assert.equal(
    messages.at(-2)?.content,
    `[Haru (human) in general place_id=${place} message_id=m-1 — fyi, no reply needed] みんなへの周知`,
  );
  // Another secretary is named as one; brackets in names cannot close the
  // marker early, so a directive after it still parses.
  assert.equal(
    messages.at(-1)?.content,
    `[Shiro bot (personality_agent) in general place_id=${place} message_id=m-2] 見てくれる？`,
  );
  // Non-Messaging inputs keep the plain actor marker without place refs.
  const plain = assemble([], {
    ...input,
    payload: { text: "hi" },
    actor_kind: "human",
    source_surface: "test",
  });
  assert.equal(plain.at(-1)?.content, "[human] hi");
});

test("assemble and journal render message change updates, not rewrites", () => {
  const place = "0190a8a0-0000-7000-8000-000000000001";
  // A delivered edit renders the change cue on the marker; the original
  // journaled input_received keeps its own frozen rendering.
  const journaled: Event[] = [
    {
      persona_id: PERSONA,
      seq: 1,
      turn_id: "t1",
      kind: "input_received",
      payload: {
        text: "元の相談",
        actor_kind: "human",
        actor_display: "Haru",
        source_surface: "messaging",
        thread_id: place,
        place_name: "general",
        place_kind: "channel",
        message_id: "m-1",
        attention: "reply",
      },
      created_at: new Date().toISOString(),
    },
    {
      persona_id: PERSONA,
      seq: 2,
      turn_id: "t2",
      kind: "input_received",
      payload: {
        text: "訂正後です",
        actor_kind: "human",
        actor_display: "Haru",
        source_surface: "messaging",
        thread_id: place,
        place_name: "general",
        place_kind: "channel",
        message_id: "m-1",
        message_change: "edited",
        attention: "reply",
      },
      created_at: new Date().toISOString(),
    },
  ];
  const tombstone = {
    persona_id: PERSONA,
    input_id: "messaging:e-3",
    kind: "message",
    payload: {
      actor: { kind: "human", display_name: "Haru" },
      place: { id: place, kind: "channel", name: "general" },
      message_id: "m-1",
      message_change: "deleted",
    },
    actor_kind: "human",
    actor_id: "h-1",
    source_surface: "messaging",
    thread_id: place,
    occurred_at: new Date().toISOString(),
    attention: "observe" as const,
    status: "queued" as const,
    claimed_generation: null,
    turn_id: null,
    created_at: new Date().toISOString(),
    done_at: null,
    not_before: null,
  };
  const messages = assemble(journaled, tombstone);
  assert.equal(
    messages.at(-3)?.content,
    `[Haru (human) in general place_id=${place} message_id=m-1] 元の相談`,
  );
  assert.equal(
    messages.at(-2)?.content,
    `[Haru (human) in general place_id=${place} message_id=m-1 — edited] 訂正後です`,
  );
  // A tombstone input carries no text — the marker itself reports it.
  const last = messages.at(-1)?.content ?? "";
  assert.ok(last.includes("— deleted"), `tombstone marker: ${last}`);
  assert.ok(last.includes("message_id=m-1"), `tombstone marker: ${last}`);
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

test("tool results feed back into a truthful final reply (multi-round)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-15", "remember my bike");
  const provider = new ScriptedProvider({
    rounds: [
      {
        text: "noting that",
        calls: [{ tool: "journal.note", request: { text: "has a red bike" } }],
      },
      { text: "Done — I noted your red bike." },
    ],
  });
  const s = new Secretary(cfg(state, "h", { provider }));
  await s.start();
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");

  assert.deepEqual(provider.consultations, [0, 1]);
  // The second consultation carries the assistant tool_calls plus the
  // committed tool result — the reply is informed by what actually ran.
  const round1 = provider.requests[1]!;
  const toolMsg = round1.messages.find((m) => m.role === "tool")!;
  assert.equal(toolMsg.toolCallId, "call-0-0");
  assert.equal(toolMsg.name, "journal.note");
  assert.match(toolMsg.content, /seq/);
  const assistantMsg = round1.messages.find(
    (m) => m.role === "assistant" && m.toolCalls?.length,
  )!;
  assert.equal(assistantMsg.toolCalls![0]!.name, "journal.note");

  const evs = await state.events(PERSONA, 0);
  // The note event lands at claim time (atomic with the effect); the turn's
  // own events land at commit. Assert structure, not raw seq interleave.
  const kinds = evs.map((e) => e.kind);
  assert.equal(kinds.filter((k) => k === "note").length, 1);
  assert.equal(kinds.filter((k) => k === "tool_call").length, 1);
  assert.equal(kinds.filter((k) => k === "tool_result").length, 1);
  assert.equal(kinds.filter((k) => k === "assistant_message").length, 2);
  assert.ok(
    kinds.indexOf("tool_call") < kinds.indexOf("tool_result"),
    "tool_call journals before its result",
  );
  assert.equal(
    evs[evs.length - 1]!.payload.text,
    "Done — I noted your red bike.",
    "the final assistant message is the post-tool reply",
  );
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(
    (outbox[0]!.payload as { output: { text: string } }).output.text,
    "Done — I noted your red bike.",
  );
});

test("plan rounds are append-only across attempts (lost save before round 1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-16", "hi");
  const gen = (await state.acquireWriter(PERSONA, "h", 30_000)).generation;
  const { turn } = await state.loadTurn(PERSONA, gen, "t-1", 10);
  const r0 = {
    turnId: turn!.turn_id,
    round: 0,
    text: "r0",
    calls: [{ tool: "journal.note", request: { text: "n0" } }],
    usage: {},
  };
  assert.equal((await state.savePlan(PERSONA, gen, r0)).created, true);
  // Round 1 appended by a later attempt of the same input's lineage.
  const r1 = { ...r0, round: 1, text: "r1", calls: [] };
  assert.equal((await state.savePlan(PERSONA, gen, r1)).created, true);
  // Resaving a recorded round replays; a different decision conflicts.
  assert.equal((await state.savePlan(PERSONA, gen, r0)).created, false);
  await assert.rejects(
    state.savePlan(PERSONA, gen, { ...r0, text: "different" }),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );
  // Skipping a round conflicts.
  await assert.rejects(
    state.savePlan(PERSONA, gen, { ...r0, round: 3 }),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );
  // Claims address flat positions across rounds.
  await state.savePlan(PERSONA, gen, { ...r0, round: 2, text: "r2", calls: [{ tool: "journal.note", request: { text: "n2" } }] });
  const claim = await state.claimOperation(PERSONA, gen, {
    operationId: "op-1",
    turnId: turn!.turn_id,
    tool: "journal.note",
    callIndex: 1,
    request: { text: "n2" },
  });
  assert.equal(claim.fresh, true);
});

test("attempt cap + budget: transient provider retries within the budget, then resolves", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-17", "hi");
  let consultations = 0;
  const down: ModelProvider = {
    name: "down",
    stream() {
      consultations++;
      throw new Error("provider down");
    },
  };
  const s = new Secretary(
    cfg(state, "h", {
      provider: down,
      maxAttempts: 1,
    }),
  );
  const input = () => state.inputs.find((i) => i.input_id === "in-17")!;
  await s.start();
  assert.equal(await s.step(), "turn"); // attempt 1: fails, retryable within budget
  assert.equal(input().status, "queued");
  input().not_before = null;
  // Attempt 2 is already past maxAttempts — but the provider-retry budget
  // is still open, so a transient outage keeps retrying instead of
  // discarding the request (F1).
  assert.equal(await s.step(), "turn");
  assert.equal(consultations, 2);
  assert.equal(input().status, "queued");
  // Budget elapsed: the attempt cap resolves the input honestly.
  input().not_before = null;
  input().created_at = new Date(Date.now() - 31 * 60_000).toISOString();
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");
  assert.equal(consultations, 2, "no consult past the expired budget");
  assert.equal(input().status, "done", "input ends once the budget closes");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-17" && t.status === "failed" &&
      /attempt cap/.test(t.error ?? ""),
  )!;
  assert.match(failed.error ?? "", /attempt cap 1 exceeded/);
});

test("short provider outage: retryable failures recover and complete within the budget (F1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-17b", "hi");
  let consultations = 0;
  const flaky: ModelProvider = {
    name: "flaky",
    async *stream() {
      consultations++;
      if (consultations <= 2) {
        throw new ModelError("provider overloaded", {
          retryable: true,
          retryAfterMs: 42_000,
        });
      }
      yield { type: "text", delta: "recovered answer" };
      yield { type: "done", usage: {} };
    },
  };
  // Default maxAttempts 5, default budget — no test overrides on policy.
  const s = new Secretary(cfg(state, "h", { provider: flaky }));
  await s.start();
  const input = () => state.inputs.find((i) => i.input_id === "in-17b")!;
  assert.equal(await s.step(), "turn"); // attempt 1: retryable
  assert.equal(input().status, "queued");
  // The provider's Retry-After hint paces the durable requeue.
  const nb = Date.parse(input().not_before!);
  assert.ok(
    nb >= Date.now() + 40_000 && nb <= Date.now() + 120_000,
    `not_before honors retry_after_ms, got ${input().not_before}`,
  );
  input().not_before = null;
  assert.equal(await s.step(), "turn"); // attempt 2: still retryable
  assert.equal(input().status, "queued");
  input().not_before = null;
  assert.equal(await s.step(), "turn"); // provider healed — completes
  assert.equal(input().status, "done");
  const out = await state.outbox(PERSONA, 0);
  const reply = out.find((o) => o.payload.input_id === "in-17b");
  assert.equal(reply!.kind, "turn_completed");
});

test("a deterministic provider rejection terminates immediately, not after the cap (F1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-17c", "hi");
  let consultations = 0;
  const bad: ModelProvider = {
    name: "bad",
    stream() {
      consultations++;
      throw new ModelError("model request failed: 401 invalid api key", {
        retryable: false,
      });
    },
  };
  const s = new Secretary(cfg(state, "h", { provider: bad }));
  await s.start();
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");
  assert.equal(consultations, 1, "no retries for a permanent rejection");
  const input = state.inputs.find((i) => i.input_id === "in-17c")!;
  assert.equal(input.status, "done");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-17c" && t.status === "failed",
  )!;
  assert.match(failed.error ?? "", /401 invalid api key/);
  const out = await state.outbox(PERSONA, 0);
  assert.ok(
    out.some(
      (o) => o.kind === "turn_failed" && o.payload.input_id === "in-17c",
    ),
  );
});

test("lease renews while a model call is in flight", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-18", "slow question");
  let renewals = 0;
  const counting: StateClient = Object.create(state, {
    renewWriter: {
      value: async (
        ...args: Parameters<StateClient["renewWriter"]>
      ): ReturnType<StateClient["renewWriter"]> => {
        renewals++;
        return state.renewWriter(...args);
      },
    },
  });
  const slow: ModelProvider = {
    name: "slow",
    async *stream() {
      // Longer than two renewal ticks — the lease must outlive the stream.
      await new Promise((r) => setTimeout(r, 200));
      yield { type: "text", delta: "done thinking" };
      yield { type: "done", usage: {} };
    },
  };
  const s = new Secretary(
    cfg(counting, "h", {
      provider: slow,
      leaseTtlMs: 30_000,
      renewEveryMs: 50,
    }),
  );
  await s.start();
  const before = renewals;
  assert.equal(await s.step(), "turn");
  assert.ok(
    renewals > before,
    "the lease renewed at least once while the model call was in flight",
  );
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1);
});

// --- CR3-B1/B2 repairs ported from 0cd5410 (deterministic tool data) ------

test("a decision containing NUL data fails the input non-retryable; the next input completes (CR3-B1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-20", "hi");
  // First consultation decides a call whose request can never persist
  // (jsonb cannot hold NUL); the second consultation is a clean reply.
  let consultations = 0;
  const seq: ModelProvider = {
    name: "seq",
    async *stream(_req: ModelRequest) {
      consultations++;
      yield { type: "text", delta: "r" };
      if (consultations === 1) {
        yield {
          type: "tool_call",
          call: {
            id: "c0",
            name: "journal.note",
            arguments: { text: "a\u0000b" },
          },
        };
      }
      yield { type: "done", usage: {} };
    },
  };
  const s = new Secretary(cfg(state, "h", { provider: seq }));
  await s.start();
  assert.equal(await s.step(), "turn");
  // The impossible decision is not retried: the turn is failed and the
  // input resolves instead of blocking every later input.
  assert.equal(consultations, 1, "savePlan 400 is not retried");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-20" && t.status === "failed",
  );
  assert.ok(failed, "turn must record the non-retryable failure");
  assert.match(
    failed!.error ?? "",
    /decision could not be recorded|diverged retry/,
  );
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-20")!.status,
    "done",
  );
  // The requester sees the request ended — the failure is the reply.
  const out = await state.outbox(PERSONA, 0);
  const failedRecord = out.find((o) => o.kind === "turn_failed");
  assert.ok(failedRecord, "a terminal failure must reach the outbox");
  assert.equal(failedRecord!.payload.input_id, "in-20");
  // The queue is unblocked: a subsequent normal input completes.
  state.addInput(PERSONA, "in-21", "hello");
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-21")!.status,
    "done",
  );
});

test("schedule.set with an invalid miss_policy is a recorded tool error, not a retried failure (CR3-B1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-22", "remind me");
  const bad = new ScriptedProvider({
    rounds: [
      {
        text: "scheduling",
        calls: [
          {
            tool: "schedule.set",
            request: {
              wake_at: "2030-01-01T00:00:00Z",
              miss_policy: "bogus",
            },
          },
        ],
      },
      { text: "could not set that reminder" },
    ],
  });
  const s = new Secretary(cfg(state, "h", { provider: bad }));
  await s.start();
  assert.equal(await s.step(), "turn");
  const evs = await state.events(PERSONA, 0);
  const tr = evs.find((e) => e.kind === "tool_result" && e.payload.error);
  assert.ok(tr, "the tool error must be observable in the journal");
  assert.match(String(tr!.payload.error), /miss_policy/);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-22")!.status,
    "done",
    "the input resolves — no poison retry loop",
  );
  assert.equal(state.schedules.size, 0, "no schedule was created");
  // The round-1 consultation saw the recorded tool error.
  const r1 = bad.requests[1]!;
  assert.match(
    r1.messages.find((m) => m.role === "tool")!.content,
    /miss_policy/,
  );
});

test("schedule.set schedule_id reuse: identical pending replays; different contents or a fired row is a tool error (CR3-B2)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const s = new Secretary(cfg(state));
  await s.start();
  const gen = s.generation!;
  const req1 = {
    schedule_id: "rem",
    wake_at: "2000-01-01T00:00:00Z",
    payload: { text: "hi" },
    miss_policy: "coalesce",
  };
  const setSchedule = async (
    inputId: string,
    turnId: string,
    request: Record<string, unknown>,
  ) => {
    state.addInput(PERSONA, inputId, "x");
    await state.loadTurn(PERSONA, gen, turnId, 10);
    await state.savePlan(PERSONA, gen, {
      turnId,
      round: 0,
      text: "r",
      calls: [{ tool: "schedule.set", request }],
      usage: {},
    });
    try {
      return await state.claimOperation(PERSONA, gen, {
        operationId: `${turnId}:op:0`,
        turnId,
        tool: "schedule.set",
        callIndex: 0,
        request,
      });
    } finally {
      // The secretary always resolves the turn after a claim — a 400 is
      // recorded as a tool_result error and the turn commits complete.
      await state.commitTurn(PERSONA, turnId, gen, {
        outcome: "complete",
        events: [],
      });
    }
  };

  // in-30 creates the schedule.
  const c1 = await setSchedule("in-30", "t-1", req1);
  assert.equal(c1.fresh, true);

  // in-31 reuses the id with different contents → explicit 400; the
  // existing row is untouched.
  await assert.rejects(
    setSchedule("in-31", "t-2", { ...req1, wake_at: "2030-01-01T00:00:00Z" }),
    (e: unknown) => e instanceof StateError && e.status === 400,
  );
  const kept = state.schedules.get(`${PERSONA}|rem`)!;
  assert.equal(
    kept.wake_at,
    new Date("2000-01-01T00:00:00Z").toISOString(),
  );
  assert.equal(kept.status, "pending");

  // in-32 reuses the id with identical contents while pending → the
  // existing row is returned; no duplicate, no false "newly decided".
  const c3 = await setSchedule("in-32", "t-3", req1);
  assert.equal(c3.fresh, true);
  assert.equal(
    [...state.schedules.values()].filter((x) => x.schedule_id === "rem")
      .length,
    1,
  );

  // Fire it: an identical reuse now names a dead row → honest error.
  const fired = await state.dispatchSchedules(PERSONA, gen);
  assert.equal(fired.length, 1);
  await assert.rejects(setSchedule("in-33", "t-4", req1), (e: unknown) => {
    return e instanceof StateError && e.status === 400;
  });
});

test("the model-visible schedule.set spec does not expose schedule_id", async () => {
  const spec = toolSpecs().find((t) => t.name === "schedule.set")!;
  const props = Object.keys(
    (spec.parameters as { properties: Record<string, unknown> }).properties,
  );
  assert.deepEqual(props.sort(), ["payload", "wake_at"]);
});

// --- Opus closure residuals (INTEGRATION-FOLLOWUP) -------------------------

test("a provider error containing NUL still persists as a recorded terminal failure", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-40", "hi");
  const poisoned: ModelProvider = {
    name: "poisoned",
    async *stream() {
      throw new Error("provider blew up\0with NUL");
    },
  };
  const s = new Secretary(
    cfg(state, "h", {
      provider: poisoned,
      maxAttempts: 1,
      providerRetryBudgetMs: 0, // budget closed → failure is terminal
    }),
  );
  await s.start();
  assert.equal(await s.step(), "turn");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-40" && t.status === "failed",
  );
  assert.ok(failed, "the failure itself must persist");
  assert.ok(!(failed!.error ?? "").includes("\0"), "error is sanitized");
  assert.match(failed!.error ?? "", /provider blew up/);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-40")!.status,
    "done",
    "the input resolves instead of poisoning the queue",
  );
  const out = await state.outbox(PERSONA, 0);
  assert.ok(
    out.some(
      (o) => o.kind === "turn_failed" && o.payload.input_id === "in-40",
    ),
    "requester sees a terminal turn_failed record",
  );
});

test("a NUL in reply text is normalized, not fatal; tool args stay strict", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-41", "hi");
  const nulReply: ModelProvider = {
    name: "nul-reply",
    async *stream() {
      yield { type: "text", delta: "hello\0 there" };
      yield { type: "done", usage: {} };
    },
  };
  const s = new Secretary(cfg(state, "h", { provider: nulReply }));
  await s.start();
  assert.equal(await s.step(), "turn");
  const input = state.inputs.find((i) => i.input_id === "in-41")!;
  assert.equal(input.status, "done", "a NUL reply must not kill the work");
  const out = await state.outbox(PERSONA, 0);
  const reply = out.find(
    (o) => o.kind === "turn_completed" && o.payload.input_id === "in-41",
  )!;
  assert.equal(
    (reply.payload.output as { text: string }).text,
    "hello there",
    "stored reply text has the NUL normalized out",
  );
});

test("a deterministic commit rejection resolves as a recorded failure, not a poison loop (fresh F1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-40", "hi");
  // The store deterministically rejects the first commit (e.g. payload
  // the database cannot store); the core must record an honest failure —
  // not leave the input claimed by an un-finalizable running turn.
  const rejectOnce: StateClient = Object.create(state, {
    commitTurn: {
      value: async (
        ...args: Parameters<StateClient["commitTurn"]>
      ): ReturnType<StateClient["commitTurn"]> => {
        const req = args[3];
        if (req.outcome === "complete") {
          throw new StateError(400, "commit contains a NUL byte jsonb cannot store");
        }
        return state.commitTurn(...args);
      },
    },
  });
  const s = new Secretary(cfg(rejectOnce, "h", {
    provider: new ScriptedProvider({ text: "reply", calls: [] }),
  }));
  await s.start();
  assert.equal(await s.step(), "turn");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-40" && t.status === "failed",
  );
  assert.ok(failed, "un-storable commit must resolve as a recorded failure");
  assert.match(failed!.error ?? "", /commit rejected deterministically/);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-40")!.status,
    "done",
    "input resolves — no running-turn poison loop",
  );
  // The journal still carries the attempt's events (scrubbed).
  assert.ok(
    (await state.events(PERSONA, 0)).some(
      (e) => e.kind === "assistant_message",
    ),
    "journal keeps the committed events",
  );
});

test("an un-storable provider error commits scrubbed and retryable — with backoff, then recovers (fresh F1+F2)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-41", "hi");
  let calls = 0;
  const failingThenFine: ModelProvider = {
    name: "flaky",
    async *stream(_req: ModelRequest) {
      calls++;
      if (calls === 1) throw new Error("provider exploded \u0000");
      yield { type: "text", delta: "recovered" };
      yield { type: "done", usage: {} };
    },
  };
  const s = new Secretary(cfg(state, "h", { provider: failingThenFine }));
  await s.start();
  assert.equal(await s.step(), "turn");
  // The NUL-bearing error was normalized and committed retryable: the
  // input is queued with a backoff delay, not hot-looping.
  const bad = state.inputs.find((i) => i.input_id === "in-41")!;
  assert.equal(bad.status, "queued");
  assert.ok(bad.not_before !== null, "requeue carries not_before backoff");
  assert.ok(Date.parse(bad.not_before) > Date.now());
  assert.equal(calls, 1, "no immediate retry — backoff parks the input");
  // While in-41 is parked, a later queued input is claimed and completes.
  state.addInput(PERSONA, "in-42", "later");
  assert.equal(await s.step(), "turn");
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-42")!.status,
    "done",
    "later input is not starved by the retrying one",
  );
  // Once the backoff expires the failed input retries and recovers.
  bad.not_before = new Date(Date.now() - 1).toISOString();
  assert.equal(await s.step(), "turn");
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-41")!.status,
    "done",
    "the retryable input recovers once its delay passes",
  );
  const out = (await state.outbox(PERSONA, 0)).find(
    (o) => o.payload.input_id === "in-41",
  );
  assert.equal(
    (out!.payload as { output: { text: string } }).output.text,
    "recovered",
  );
});

test("a run() loop rides out a transient state outage with backoff (review-A F1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  // Fail every state call with a transport-style error until switched off.
  let down = true;
  const flaky: StateClient = new Proxy(state, {
    get(target, prop, recv) {
      const v = Reflect.get(target, prop, recv);
      if (typeof v !== "function" || prop === "addPersona" || prop === "addInput") return v;
      return async (...args: unknown[]) => {
        if (down) throw new TypeError("fetch failed");
        return (v as (...a: unknown[]) => unknown).apply(target, args);
      };
    },
  });
  const s = new Secretary(
    cfg(flaky, "h", { provider: new ScriptedProvider({ text: "ok", calls: [] }) }),
  );
  const ac = new AbortController();
  const done = s.run(ac.signal);
  // Let a couple of backoff cycles happen while the service is "down".
  await new Promise((r) => setTimeout(r, 1_300));
  state.addInput(PERSONA, "in-50", "hello");
  down = false;
  const deadline = Date.now() + 10_000;
  while (
    state.inputs.find((i) => i.input_id === "in-50")!.status !== "done" &&
    Date.now() < deadline
  ) {
    await new Promise((r) => setTimeout(r, 50));
  }
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-50")!.status,
    "done",
    "the input completes after the outage ends — no permanent exit",
  );
  ac.abort();
  await done;
});

test("a >body-limit provider error still records a bounded honest failure (F-B1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-50", "hi");
  // Multibyte + control characters, ~1.5 MB total — over the server
  // body limit FakeState now enforces.
  const huge = "ø😀".repeat(200_000);
  let calls = 0;
  const giant: ModelProvider = {
    name: "giant",
    async *stream() {
      calls++;
      if (calls === 1) throw new Error(`provider exploded ${huge}`);
      yield { type: "text", delta: "fine" };
      yield { type: "done", usage: {} };
    },
  };
  const s = new Secretary(cfg(state, "h", { provider: giant }));
  await s.start();
  assert.equal(await s.step(), "turn");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-50" && t.status === "failed",
  );
  assert.ok(failed, "the oversized error must resolve to a recorded failure");
  const recorded = failed!.error ?? "";
  assert.ok(
    new TextEncoder().encode(recorded).length < 10_000,
    `recorded error must be bounded, got ${recorded.length} chars`,
  );
  // The error is bounded at the source, so the FIRST commit already fits —
  // no server rejection is involved.
  assert.match(recorded, /truncated/, "marks truncation explicitly");
  assert.match(recorded, /provider exploded/, "keeps the useful reason");
  const in50 = state.inputs.find((i) => i.input_id === "in-50")!;
  assert.equal(in50.status, "queued", "retryable disposition survives");
  assert.ok(in50.not_before !== null, "requeue carries backoff");
  // A later queued input is not starved by the failing one.
  state.addInput(PERSONA, "in-51", "later");
  assert.equal(await s.step(), "turn");
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-51")!.status,
    "done",
  );
  // Provider healed: the retryable input completes on its next attempt.
  in50.not_before = null;
  assert.equal(await s.step(), "turn");
  assert.equal(in50.status, "done");
  const ob50 = (await state.outbox(PERSONA, 0)).find(
    (o) => o.payload.input_id === "in-50",
  );
  assert.equal(ob50!.kind, "turn_completed");
});

test("an oversized complete commit downgrades through the minimal tier (F-B1)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-52", "hi");
  // A ~1.5 MB reply makes both the original commit AND the scrubbed
  // commit (events still carry the giant assistant_message) un-storable;
  // only the minimal failure commit can land.
  const s = new Secretary(cfg(state, "h", {
    provider: new ScriptedProvider({ text: "x".repeat(1_500_000) }),
  }));
  await s.start();
  assert.equal(await s.step(), "turn");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-52" && t.status === "failed",
  );
  assert.ok(failed, "oversized complete resolves as a recorded failure");
  const recorded = failed!.error ?? "";
  assert.ok(recorded.length < 10_000, "recorded error bounded");
  assert.match(recorded, /read body/);
  assert.match(recorded, /could not be stored/);
  const in52 = state.inputs.find((i) => i.input_id === "in-52")!;
  assert.equal(in52.status, "done", "input finalizes — no poison loop");
  // No fabricated success: this branch records a visible turn_failed —
  // the honest reply — and never a turn_completed.
  const ob52 = (await state.outbox(PERSONA, 0)).filter(
    (o) => o.payload.input_id === "in-52",
  );
  assert.equal(ob52.length, 1);
  assert.equal(ob52[0]!.kind, "turn_failed");
});

// --- Fresh-review F3/F4/F6 repair coverage ---------------------------------

test("restart with a dead predecessor's lease waits out the TTL instead of dying (F3)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-60", "hi");
  // A prior process died without releasing: its lease still reads live.
  await state.acquireWriter(PERSONA, "local-dead-pid", 300);
  const logs: string[] = [];
  const s = new Secretary(
    cfg(state, "local-new-pid", {
      leaseTtlMs: 300,
      pollIntervalMs: 5,
      log: (m) => logs.push(m),
    }),
  );
  const ac = new AbortController();
  const done = s.run(ac.signal);
  const deadline = Date.now() + 10_000;
  for (;;) {
    const input = state.inputs.find((i) => i.input_id === "in-60")!;
    if (input.status === "done") break;
    assert.ok(Date.now() < deadline, "dead predecessor's lease never expired");
    await new Promise((r) => setTimeout(r, 50));
  }
  ac.abort();
  await done;
  assert.ok(
    logs.some((m) => m.includes("waiting for expiry")),
    "the held lease was waited out, not fatal",
  );
  const out = await state.outbox(PERSONA, 0);
  assert.equal(out[0]!.kind, "turn_completed");
});

test("a live holder's lease is never stolen — run() yields honestly (F3)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-61", "hi");
  // A genuinely live holder keeps renewing; its lease outlives the
  // newcomer's wait window.
  const holder = new Secretary(cfg(state, "local-live", { leaseTtlMs: 2_000 }));
  await holder.start();
  const newcomer = new Secretary(
    cfg(state, "local-new", { leaseTtlMs: 200, pollIntervalMs: 5 }),
  );
  const ac = new AbortController();
  await assert.rejects(
    newcomer.run(ac.signal),
    (e: unknown) =>
      e instanceof StateError && e.status === 409,
    "a live holder is not displaced — the newcomer yields",
  );
  // The live holder's work is untouched.
  assert.equal(await holder.step(), "turn");
  assert.equal(state.inputs.find((i) => i.input_id === "in-61")!.status, "done");
  await holder.stop();
});

test("retry sleeps do not accumulate abort listeners on the run signal (F4)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  let calls = 0;
  const outage: StateClient = Object.create(state, {
    loadTurn: {
      value: async (
        ...args: Parameters<StateClient["loadTurn"]>
      ): ReturnType<StateClient["loadTurn"]> => {
        calls++;
        if (calls <= 3) throw new TypeError("fetch failed"); // transport outage
        return state.loadTurn(...args);
      },
    },
  });
  const s = new Secretary(
    cfg(outage, "h", { pollIntervalMs: 5, provider: new MockProvider() }),
  );
  const ac = new AbortController();
  const done = s.run(ac.signal);
  let maxListeners = 0;
  const deadline = Date.now() + 10_000;
  while (calls <= 3) {
    maxListeners = Math.max(
      maxListeners,
      getEventListeners(ac.signal, "abort").length,
    );
    assert.ok(Date.now() < deadline, "transient outage never recovered");
    await new Promise((r) => setTimeout(r, 30));
  }
  // Let one more step land, then stop.
  await new Promise((r) => setTimeout(r, 100));
  maxListeners = Math.max(
    maxListeners,
    getEventListeners(ac.signal, "abort").length,
  );
  ac.abort();
  await done;
  assert.ok(
    maxListeners <= 1,
    `abort listeners accumulated across retries: ${maxListeners}`,
  );
});

test("an unknown recurring in-turn error resolves as an honest failure after 3 strikes (F6)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-62", "hi");
  // claimOperation throws a defect-class error (not StateError, not transport):
  // without a bound, the same running turn would retry forever in silence.
  const buggy: StateClient = Object.create(state, {
    claimOperation: {
      value: async (): Promise<never> => {
        throw new RangeError("corrupt operation record");
      },
    },
  });
  const s = new Secretary(
    cfg(buggy, "h", {
      provider: new ScriptedProvider({
        text: "",
        calls: [{ tool: "journal.note", request: { text: "x" } }],
      }),
    }),
  );
  await s.start();
  await assert.rejects(s.step(), /corrupt operation record/); // strike 1
  await assert.rejects(s.step(), /corrupt operation record/); // strike 2
  assert.equal(await s.step(), "turn"); // strike 3 → recorded failure
  const input = state.inputs.find((i) => i.input_id === "in-62")!;
  assert.equal(input.status, "done", "defect resolves, not an infinite loop");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-62" && t.status === "failed",
  )!;
  assert.match(failed.error ?? "", /recurring internal error: corrupt/);
  const out = await state.outbox(PERSONA, 0);
  assert.ok(
    out.some(
      (o) => o.kind === "turn_failed" && o.payload.input_id === "in-62",
    ),
  );
});

test("a rejected claim still journals its tool_call before the tool_result (F6)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-63", "hi");
  const s = new Secretary(
    cfg(state, "h", {
      provider: new ScriptedProvider({
        rounds: [
          {
            text: "",
            calls: [{ tool: "no.such.tool", request: { x: 1 } }],
          },
          { text: "could not do it", calls: [] },
        ],
      }),
    }),
  );
  await s.start();
  assert.equal(await s.step(), "turn");
  const evs = await state.events(PERSONA, 0);
  const callIdx = evs.findIndex(
    (e) => e.kind === "tool_call" && e.payload.tool === "no.such.tool",
  );
  const resIdx = evs.findIndex(
    (e) => e.kind === "tool_result" && e.payload.tool === "no.such.tool",
  );
  assert.ok(callIdx >= 0, "rejected call is still journaled as a decision");
  assert.ok(
    resIdx > callIdx,
    "the rejection result follows its call symmetrically",
  );
});

// --- Final-review NF2/NF3/NF4 repair coverage --------------------------------

test("a corrupt 200 body from the state service classifies as transient (NF2)", async () => {
  const client = new HttpStateClient("http://unused", "tok", async () => ({
    ok: true,
    status: 200,
    json: async () => {
      throw new SyntaxError("Unexpected end of JSON input");
    },
    text: async () => "garbage",
  }));
  await assert.rejects(
    client.personaState(PERSONA),
    (e: unknown) =>
      e instanceof StateError && e.status === 503 && /unreadable 200/.test(e.message),
    "an unreadable ok body surfaces as a transient 5xx",
  );
});

test("an unreadable state response retries in-process rather than exiting (NF2)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-70", "hi");
  let corrupt = true;
  const flicker: StateClient = Object.create(state, {
    loadTurn: {
      value: async (
        ...args: Parameters<StateClient["loadTurn"]>
      ): ReturnType<StateClient["loadTurn"]> => {
        if (corrupt) {
          corrupt = false;
          throw new StateError(
            503,
            "state service returned an unreadable 200 body: Unexpected end of JSON input",
          );
        }
        return state.loadTurn(...args);
      },
    },
  });
  const s = new Secretary(cfg(flicker, "h", { pollIntervalMs: 5 }));
  const ac = new AbortController();
  const done = s.run(ac.signal);
  const deadline = Date.now() + 10_000;
  for (;;) {
    if (state.inputs.find((i) => i.input_id === "in-70")!.status === "done") {
      break;
    }
    assert.ok(Date.now() < deadline, "corrupt response never recovered");
    await new Promise((r) => setTimeout(r, 30));
  }
  ac.abort();
  await done;
});

test("a transient error on the post-deadline acquire keeps retrying (NF3)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-71", "hi");
  const t0 = Date.now();
  // A live holder whose lease outlives the newcomer's wait window
  // (ttl 600 > newcomer deadline ~400) but then lapses.
  await state.acquireWriter(PERSONA, "local-live", 600);
  // Inject a transport failure on the acquire immediately after the
  // first post-deadline 409 — i.e. on the loop's "final attempt".
  let sawLateConflict = false;
  let injected = false;
  const flicker: StateClient = Object.create(state, {
    acquireWriter: {
      value: async (
        ...args: Parameters<StateClient["acquireWriter"]>
      ): ReturnType<StateClient["acquireWriter"]> => {
        if (sawLateConflict && !injected) {
          injected = true;
          throw new TypeError("fetch failed");
        }
        try {
          return await state.acquireWriter(...args);
        } catch (e) {
          if (
            e instanceof StateError &&
            e.status === 409 &&
            Date.now() - t0 >= 350
          ) {
            sawLateConflict = true;
          }
          throw e;
        }
      },
    },
  });
  const s = new Secretary(
    cfg(flicker, "local-new", { leaseTtlMs: 200, pollIntervalMs: 5 }),
  );
  const ac = new AbortController();
  const done = s.run(ac.signal);
  const deadline = Date.now() + 10_000;
  for (;;) {
    if (state.inputs.find((i) => i.input_id === "in-71")!.status === "done") {
      break;
    }
    assert.ok(
      Date.now() < deadline,
      "post-deadline transient error exited the process",
    );
    await new Promise((r) => setTimeout(r, 50));
  }
  assert.ok(injected, "the post-deadline attempt was exercised");
  ac.abort();
  await done;
});

test("SIGTERM while waiting out a held lease exits cleanly (NF4)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  // A live holder whose lease outlives the whole test.
  await state.acquireWriter(PERSONA, "local-live", 30_000);
  const logs: string[] = [];
  const s = new Secretary(
    cfg(state, "local-new", {
      leaseTtlMs: 400,
      pollIntervalMs: 5,
      log: (m) => logs.push(m),
    }),
  );
  const ac = new AbortController();
  const done = s.run(ac.signal);
  await new Promise((r) => setTimeout(r, 250)); // mid lease-wait
  ac.abort();
  await assert.doesNotReject(done, "abort during lease-wait is a clean stop");
  assert.ok(logs.some((m) => m.includes("waiting for expiry")));
});

test("a near-limit input with a transient provider error stays retryable (opus F1+F2)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  // Near-limit input (~1.04 MB): on the single-round contract the
  // input_received event pushed every event-carrying commit tier over
  // the body limit, so only the minimal tier could land — and it had to
  // preserve the retryable disposition. On this multi-round branch a
  // retryable failure commits events:[] (no partial journal), so the
  // commit is ~10 KB and the cascade is structurally unreached — the
  // same guarantee holds by construction: the disposition flows to the
  // one commit that runs. The observable contract is identical: queued,
  // backed off, later input progresses, heals to exactly one reply.
  state.addInput(PERSONA, "in-53", "x".repeat(1_042_000));
  state.addInput(PERSONA, "in-54", "later");
  let calls = 0;
  const flaky: ModelProvider = {
    name: "flaky",
    async *stream() {
      calls++;
      if (calls === 1) throw new Error(`provider 503 ${"y".repeat(50_000)}`);
      yield { type: "text", delta: "recovered" };
      yield { type: "done", usage: {} };
    },
  };
  const s = new Secretary(cfg(state, "h", { provider: flaky }));
  await s.start();
  assert.equal(await s.step(), "turn");
  const in53 = state.inputs.find((i) => i.input_id === "in-53")!;
  assert.equal(
    in53.status,
    "queued",
    "minimal tier must preserve the retryable disposition",
  );
  assert.ok(in53.not_before !== null, "requeue carries backoff");
  const failed = [...state.turns.values()].find(
    (t) => t.input_id === "in-53" && t.status === "failed",
  );
  const recorded = failed!.error ?? "";
  assert.ok(recorded.length < 10_000, "recorded error bounded");
  // The provider reason survives bounded at the source; the tier-cascade
  // text ("read body; then read body") cannot appear here because a
  // retryable commit carries no events and always fits the body limit.
  assert.match(recorded, /model: provider 503/);
  // The later input is not starved while in-53 backs off.
  assert.equal(await s.step(), "turn");
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-54")!.status,
    "done",
  );
  // Backoff expired → in-53 retries and completes once.
  in53.not_before = new Date(Date.now() - 1).toISOString();
  assert.equal(await s.step(), "turn");
  assert.equal(in53.status, "done", "transient failure recovers");
  const out = (await state.outbox(PERSONA, 0)).find(
    (o) => o.payload.input_id === "in-53",
  );
  assert.equal(
    (out!.payload as { output: { text: string } }).output.text,
    "recovered",
  );
});
