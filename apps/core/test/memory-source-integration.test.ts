import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  renderJournalContext,
  estTextTokens,
  estEventTokens,
} from "../src/memory.ts";
import {
  memorySource,
  type MemoryBranch,
  type MemorySourceRange,
} from "../src/memory-branch.ts";
import type { Event } from "../src/types.ts";
const p = "01930e00-0000-7000-8000-000000000099";
function events(): Event[] {
  const rows: [string, Record<string, unknown>][] = [
    ["input_received", { text: "write two files", actor_kind: "human" }],
    ["assistant_message", { text: "I will save both", round: 0 }],
    [
      "tool_call",
      {
        tool: "file.write",
        call_id: "one",
        call_index: 0,
        route: "normal",
        request: { path: "one.txt", content_text: "雨" },
        round: 0,
      },
    ],
    ["note", { text: "while writing" }],
    [
      "tool_result",
      {
        tool: "file.write",
        call_id: "one",
        call_index: 0,
        response: { version: 1 },
        round: 0,
      },
    ],
    [
      "tool_call",
      {
        tool: "file.write",
        call_id: "two",
        call_index: 1,
        route: "normal",
        request: { path: "two.txt", content_text: "😀" },
        round: 0,
      },
    ],
    [
      "tool_result",
      {
        tool: "file.write",
        call_id: "two",
        call_index: 1,
        response: { version: 2 },
        round: 0,
      },
    ],
    ["turn_completed", {}],
  ];
  return rows.map(([kind, payload], i) => ({
    persona_id: p,
    seq: i + 1,
    turn_id: "t1",
    kind,
    payload,
    created_at: "2026-09-29T00:00:00Z",
  }));
}
function fixture(): MemoryBranch {
  const ranges: MemorySourceRange[] = [];
  const messages = [
    { role: "system" as const, content: "same parent" },
    ...renderJournalContext(events(), [], null, null, [], ranges),
  ];
  return {
    chunk: {
      persona_id: p,
      chunk_seq: 1,
      layer: 1,
      sources: [],
      first_seq: 1,
      last_seq: 8,
      est_tokens: 11000,
      status: "sealed",
      replacement: null,
      replacement_est_tokens: null,
      attempts: 0,
      interruptions: 0,
      last_error: null,
      claimed_generation: null,
      claimed_at: null,
      not_before: null,
      created_at: "now",
      prepared_at: null,
      applied_at: null,
    },
    snapshot: { messages, tools: [], ranges },
    state: null,
    revision: 0,
  };
}
test("native grouped history maps all journal events to exact messages, and source contains each message once", () => {
  const b = fixture();
  assert.deepEqual(
    b.snapshot.ranges.map((r) => r.first_seq).sort((a, z) => a - z),
    [1, 2, 3, 4, 5, 6, 7, 8],
  );
  const index = b.snapshot.ranges.find((r) => r.first_seq === 3)!
    .message_index!;
  assert.equal(
    b.snapshot.ranges.find((r) => r.first_seq === 6)!.message_index,
    index,
  );
  assert.equal(b.snapshot.messages[index]!.toolCalls!.length, 2);
  assert.equal(
    b.snapshot.messages[
      b.snapshot.ranges.find((r) => r.first_seq === 5)!.message_index!
    ]!.role,
    "tool",
  );
  const source = JSON.parse(memorySource(b));
  assert.equal(source.records.length, 5);
  const native = source.records.filter((r: any) => r.message.toolCalls);
  assert.equal(native.length, 1);
  assert.equal(native[0].message.toolCalls.length, 2);
  assert.deepEqual(native[0].journal_ranges, [
    { first_seq: 2, last_seq: 2 },
    { first_seq: 3, last_seq: 3 },
    { first_seq: 6, last_seq: 6 },
  ]);
  assert.equal(estTextTokens("雨😀"), 2);
  assert.equal(
    estEventTokens("tool_call", { text: "雨😀" }),
    Math.ceil(
      (9 +
        16 +
        new TextEncoder().encode(JSON.stringify({ text: "雨😀" })).length) /
        4,
    ),
  );
});
test("a sealed target may not consume part of a grouped native message", async () => {
  const b = fixture();
  b.chunk.last_seq = 5;
  const state = new FakeState();
  state.addPersona(p);
  state.memoryChunks.push(b.chunk);
  const gen = (await state.acquireWriter(p, "test", 60000)).generation;
  assert.equal(await state.claimMemoryBranch(p, gen, b.snapshot), null);
});

test("sealing never splits one native decision around an interleaved input", async () => {
  const state = new FakeState();
  state.addPersona(p);
  const gen = (await state.acquireWriter(p, "test", 60000)).generation;
  const rows = events();
  rows[1]!.payload.text = "x".repeat(41000);
  rows.splice(5, 0, {
    ...rows[0]!,
    turn_id: "another-turn",
    payload: { text: "arrived while recovering", input_id: "other" },
    seq: 0,
  });
  rows.push({
    ...rows[0]!,
    turn_id: "later-turn",
    payload: { text: "later boundary", input_id: "later" },
    seq: 0,
  });
  rows.forEach((e, i) => (e.seq = i + 1));
  state.eventLog.push(...rows);
  await state.memoryMaintain(p, gen);
  assert.equal(state.memoryChunks.length, 1);
  assert.equal(state.memoryChunks[0]!.last_seq, rows.length - 1);
});
