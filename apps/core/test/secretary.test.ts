import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { assemble, Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { StateError, type StateClient } from "../src/state-client.ts";
import type { Event } from "../src/types.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000001";

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

test("claim conflict: retried plan diverging from a committed effect fails loudly (F1 floor)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-7", '!journal.note {"text":"changed mind"}');
  const s = new Secretary(cfg(state));
  await s.start();
  const gen = s.generation!;
  // Simulate a prior attempt that committed this effect then crashed:
  // position 0 of in-7 already owns the idempotency key with a DIFFERENT
  // request.
  const { operation } = await state.claimOperation(PERSONA, gen, {
    operationId: "t-old:op:0",
    turnId: "t-old",
    tool: "journal.note",
    idempotencyKey: "in-7:tool:0",
    request: { text: "committed version" },
  });
  assert.equal(operation.status, "done");

  assert.equal(await s.step(), "turn"); // fails loudly, does not throw
  const turn = [...state.turns.values()].find((t) => t.input_id === "in-7")!;
  assert.equal(turn.status, "failed");
  assert.match(turn.error ?? "", /diverged retry/);
  const input = state.inputs.find((i) => i.input_id === "in-7")!;
  assert.equal(input.status, "done"); // non-retryable — no poison loop
  const notes = (await state.events(PERSONA, 0)).filter(
    (e) => e.kind === "note",
  );
  assert.equal(notes.length, 1); // no duplicate effect
  assert.equal(notes[0]!.payload.text, "committed version");
});

test("silent replay with a different request is caught client-side (pre-B2 store)", async () => {
  // A store that replays stored ops without comparing the request — the
  // current pre-fix Go behavior. The core must still detect divergence.
  const inner = new FakeState();
  inner.addPersona(PERSONA);
  inner.addInput(PERSONA, "in-8", '!journal.note {"text":"B"}');
  const lenient: StateClient = Object.create(inner, {
    claimOperation: {
      value: async (
        p: string,
        g: number,
        op: Parameters<StateClient["claimOperation"]>[2],
      ) => {
        const existing = [...inner.ops.values()].find(
          (o) =>
            o.persona_id === p &&
            o.tool === op.tool &&
            o.idempotency_key === op.idempotencyKey,
        );
        if (existing) return { operation: existing, fresh: false };
        return inner.claimOperation(p, g, op);
      },
    },
  });
  const s = new Secretary(cfg(lenient));
  await s.start();
  const gen = s.generation!;
  await inner.claimOperation(PERSONA, gen, {
    operationId: "t-old:op:0",
    turnId: "t-old",
    tool: "journal.note",
    idempotencyKey: "in-8:tool:0",
    request: { text: "A" },
  });
  assert.equal(await s.step(), "turn");
  const turn = [...inner.turns.values()].find((t) => t.input_id === "in-8")!;
  assert.equal(turn.status, "failed");
  assert.match(turn.error ?? "", /diverged retry/);
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
