import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import type { StateClient } from "../src/state-client.ts";

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
    scheduleEveryMs: 1,
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
  state.lease = {
    ...state.lease!,
    expires_at: new Date(Date.now() - 1).toISOString(),
  };

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
  await assert.rejects(b.start(), /already held/);
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
