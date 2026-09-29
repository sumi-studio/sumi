import assert from "node:assert/strict";
import { test } from "node:test";
import {
  advanceMemoryBranch,
  DEFAULT_MEMORY_POLICY,
  initialMemoryState,
} from "../src/memory-branch.ts";
import { memoryTaskFromInstruction } from "../src/memory-instructions/task.ts";
import { memoryAgentRound, memoryBranchView } from "./memory-e2e-support.mjs";

function branch(sourceLayer) {
  return {
    chunk: {
      chunk_seq: 41,
      first_seq: 10,
      last_seq: 11,
      layer: sourceLayer === 0 ? 1 : 2,
      est_tokens: 11_000,
    },
    snapshot: {
      messages: [
        { role: "system", content: "unchanged parent premises" },
        { role: "user", content: "an earlier L2 fragment outside the target" },
        { role: "user", content: "ミナ: 木曜に続きを話そう。" },
        { role: "assistant", content: "木曜にまた話そう。" },
        { role: "user", content: "current conversation outside the target" },
      ],
      tools: [
        { name: "file.read", description: "read", parameters: {} },
        { name: "file.write", description: "write", parameters: {} },
      ],
      ranges: [
        { first_seq: 1, last_seq: 2, message_index: 1, layer: 2 },
        {
          first_seq: 10,
          last_seq: 10,
          message_index: 2,
          layer: sourceLayer || undefined,
        },
        {
          first_seq: 11,
          last_seq: 11,
          message_index: 3,
          layer: sourceLayer || undefined,
        },
      ],
    },
    revision: 0,
    state: null,
  };
}

for (const [layer, transition] of [
  [0, "l0_to_l1"],
  [1, "l1_to_l2"],
  [2, "l2_to_l2"],
]) {
  test(`${transition}: e2e fixture reads task metadata and completes the actual private protocol after transcript restore`, async () => {
    let b = branch(layer);
    b.state = initialMemoryState(b, DEFAULT_MEMORY_POLICY);
    const frozen = JSON.stringify(b.snapshot);
    const phases = [];
    for (let i = 0; i < 20 && b.state.status === "running"; i++) {
      const view = memoryBranchView([
        ...b.snapshot.messages,
        ...b.state.messages,
      ]);
      assert.ok(view);
      assert.equal(view.transition, transition);
      assert.equal(view.chunk, 41);
      assert.equal(view.first_seq, 10);
      assert.equal(view.last_seq, 11);
      assert.deepEqual(view.prefix, b.snapshot.messages);
      const round = memoryAgentRound(view, { label: "fixture" });
      phases.push(round.stage);
      b.state = await advanceMemoryBranch(b, {
        text: round.text,
        calls: round.calls.map((call) => ({ ...call, route: "normal" })),
        usage: {},
      });
      b = JSON.parse(JSON.stringify(b));
    }
    assert.equal(b.state.status, "prepared");
    assert.deepEqual(phases, [
      "read_source",
      "write",
      "reread",
      "review",
      "confirm",
    ]);
    assert.equal(JSON.stringify(b.snapshot), frozen);
    assert.match(b.state.candidate.text, /木曜/);
    assert.doesNotMatch(b.state.candidate.text, /outside the target/);
  });
}

test("ordinary conversation and incomplete task metadata do not identify a memory branch", () => {
  assert.equal(
    memoryBranchView([{ role: "user", content: "記憶の整理について話そう。" }]),
    null,
  );
  assert.equal(memoryTaskFromInstruction("[memory_organization]\n{}"), null);
  assert.equal(memoryTaskFromInstruction("[memory_organization]\nnull"), null);
  assert.equal(
    memoryTaskFromInstruction(
      '[memory_organization]\n{"transition":"l0_to_l1","chunk_seq":1,"first_seq":5,"last_seq":3}',
    ),
    null,
  );
});
