import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { memoryBlockMessage, renderJournalContext } from "../src/memory.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import type { ModelProvider } from "../src/provider.ts";
const PERSONA = "01930e00-0000-7000-8000-0000000000a1";
function cfg(state: FakeState, provider: ModelProvider): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: "memory-test",
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

test("memory: a fragment names when its records were made and where they are", () => {
  const m = memoryBlockMessage({
    chunk_seq: 3,
    layer: 1,
    first_seq: 10,
    last_seq: 24,
    first_time: "2026-09-12T08:00:00Z",
    last_time: "2026-09-12T09:30:00Z",
    text: "organized",
    est_tokens: 3,
  });
  assert.ok(
    m.content.startsWith(
      "[Memory fragment recorded 2026-09-12T08:00:00Z through 2026-09-12T09:30:00Z; journal seq 10 through 24.]\n",
    ),
    m.content,
  );
  assert.match(m.content, /"chunk_seq":3/);
});

test("memory: a note follows its input, and the input is journaled exactly once", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const s = new Secretary(cfg(state, new MockProvider()));
  await s.start();
  state.addInput(PERSONA, "n0", "earlier");
  assert.equal(await s.step(), "turn");
  state.addInput(PERSONA, "n1", '!journal.note {"text":"remember x"}');
  assert.equal(await s.step(), "turn");
  const evs = await state.events(PERSONA, 0);
  const received = evs.filter(
    (e) => e.kind === "input_received" && e.payload.input_id === "n1",
  );
  assert.equal(received.length, 1, JSON.stringify(evs.map((e) => e.kind)));
  const note = evs.find((e) => e.kind === "note")!;
  assert.ok(received[0]!.seq < note.seq, "the note follows its input");
  const prior = evs.filter((e) => e.turn_id !== received[0]!.turn_id);
  assert.ok(
    prior.every((e) => e.seq < received[0]!.seq),
    "the note never lands among the previous exchange's records",
  );
  await s.stop();
});

test("all unresolved raw records and applied memory remain in the context regardless of old row/token caps", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const sec = new Secretary({
    ...cfg(state, new MockProvider()),
    contextLimit: 1,
  });
  await sec.start();
  for (let i = 0; i < 6; i++) {
    state.addInput(PERSONA, `bulk${i}`, `original-${i}:` + "x".repeat(44000));
    await sec.step({ startMemory: false });
  }
  const rendered = renderJournalContext(await state.events(PERSONA, 0, 1000));
  assert.ok(rendered.some((m) => m.content.includes("original-0")));
  state.addInput(PERSONA, "last", "next");
  const loaded = await state.loadTurn(PERSONA, sec.generation!, "last-turn", 1);
  assert.equal(loaded.omitted, null);
  assert.ok(JSON.stringify(loaded.context).includes("original-0"));
  await sec.stop();
});
