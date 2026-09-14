import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";

const PA = "01930e00-0000-7000-8000-0000000000a1";
const PB = "01930e00-0000-7000-8000-0000000000b2";

// Caller-chosen input ids may collide across personas: every lookup Go
// scopes by persona_id must scope the same way here, or one persona's
// commit mutates the other's row.
test("fake-state: colliding input_ids across personas stay scoped", async () => {
  const s = new FakeState();
  s.addPersona(PA);
  s.addPersona(PB);
  s.addInput(PA, "in-1", "for a");
  s.addInput(PB, "in-1", "for b");
  const ga = (await s.acquireWriter(PA, "h", 60_000)).generation;
  const gb = (await s.acquireWriter(PB, "h", 60_000)).generation;

  const la = await s.loadTurn(PA, ga, "t-a", 50);
  assert.equal(la.input?.input_id, "in-1");
  assert.equal(la.input?.persona_id, PA);
  const lb = await s.loadTurn(PB, gb, "t-b", 50);
  assert.equal(lb.input?.persona_id, PB);

  // Resume under the same generation replays the running turn — the
  // persona-blind lookup used to return PA's row here.
  const lb2 = await s.loadTurn(PB, gb, "t-b-again", 50);
  assert.equal(lb2.turn?.turn_id, "t-b");
  assert.equal(lb2.input?.persona_id, PB);
  assert.equal(lb2.input?.payload.text, "for b");

  await s.commitTurn(PA, "t-a", ga, {
    outcome: "complete",
    events: [
      {
        kind: "input_received",
        payload: { input_id: "in-1", kind: "message", text: "for a" },
      },
      { kind: "assistant_message", payload: { text: "hi a" } },
    ],
    output: { text: "hi a" },
  });
  const inA = s.inputs.find((i) => i.persona_id === PA && i.input_id === "in-1");
  const inB = s.inputs.find((i) => i.persona_id === PB && i.input_id === "in-1");
  assert.equal(inA?.status, "done");
  assert.equal(
    inB?.status,
    "claimed",
    "A's commit must not touch B's colliding input",
  );

  await s.commitTurn(PB, "t-b", gb, {
    outcome: "complete",
    events: [
      {
        kind: "input_received",
        payload: { input_id: "in-1", kind: "message", text: "for b" },
      },
      { kind: "assistant_message", payload: { text: "hi b" } },
    ],
    output: { text: "hi b" },
  });
  assert.equal(inB?.status, "done");
  // Each persona journaled its own receipt at its own seq 1.
  const ea = (await s.events(PA, 0)).map((e) => e.seq);
  const eb = (await s.events(PB, 0)).map((e) => e.seq);
  assert.deepEqual(ea, [1, 2]);
  assert.deepEqual(eb, [1, 2]);
});

// f74 parity: a commit carrying a receipt twice journals it once, and a
// receipt for another still-queued input links that input's marker.
test("fake-state: commit dedups input_received like the Go store", async () => {
  const s = new FakeState();
  s.addPersona(PA);
  s.addInput(PA, "in-1", "one");
  s.addInput(PA, "in-2", "two");
  const g = (await s.acquireWriter(PA, "h", 60_000)).generation;
  await s.loadTurn(PA, g, "t-1", 50);
  await s.commitTurn(PA, "t-1", g, {
    outcome: "complete",
    events: [
      {
        kind: "input_received",
        payload: { input_id: "in-1", kind: "message", text: "one" },
      },
      {
        kind: "input_received",
        payload: { input_id: "in-1", kind: "message", text: "one" },
      },
      {
        kind: "input_received",
        payload: { input_id: "in-2", kind: "message", text: "two" },
      },
      { kind: "assistant_message", payload: { text: "ok" } },
    ],
    output: { text: "ok" },
  });
  const evs = await s.events(PA, 0);
  const receipts = evs.filter((e) => e.kind === "input_received");
  assert.equal(receipts.length, 2, "one receipt per input, not per copy");
  assert.equal(receipts[0]!.payload.input_id, "in-1");
  assert.equal(receipts[1]!.payload.input_id, "in-2");
  // in-2's marker now names its journaled receipt, so its own commit does
  // not journal it again.
  const l2 = await s.loadTurn(PA, g, "t-2", 50);
  assert.equal(l2.input?.input_id, "in-2");
  await s.commitTurn(PA, "t-2", g, {
    outcome: "complete",
    events: [
      {
        kind: "input_received",
        payload: { input_id: "in-2", kind: "message", text: "two" },
      },
      { kind: "assistant_message", payload: { text: "ok2" } },
    ],
    output: { text: "ok2" },
  });
  const after = (await s.events(PA, 0)).filter(
    (e) => e.kind === "input_received",
  );
  assert.equal(after.length, 2);
});
