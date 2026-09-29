import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { eventMessage } from "../src/memory.ts";
import {
  DEFAULT_MEMORY_POLICY,
  initialMemoryState,
  type MemorySnapshot,
} from "../src/memory-branch.ts";
import type { ModelProvider, ModelRequest } from "../src/provider.ts";
import { assemble, inputReceivedEvent, Secretary } from "../src/secretary.ts";
import type { Input, MemoryChunk, Turn } from "../src/types.ts";

const persona = "01930e00-0000-7000-8000-0000000000b1";

function secretary(state: FakeState, provider: ModelProvider): Secretary {
  return new Secretary({
    personaId: persona,
    holderId: crypto.randomUUID(),
    state,
    provider,
    leaseTtlMs: 60_000,
    renewEveryMs: 10_000,
    contextLimit: 5000,
    pollIntervalMs: 1,
    scheduleEveryMs: 60_000,
    idgen: () => crypto.randomUUID(),
  });
}

test("a memory notice identifies its recovery target through a real consultation, tool claim and restarted journal", async () => {
  const state = new FakeState();
  state.addPersona(persona);
  const generation = (await state.acquireWriter(persona, "preparation", 60_000))
    .generation;
  const chunk: MemoryChunk = {
    persona_id: persona,
    chunk_seq: 7319,
    layer: 1,
    sources: [],
    first_seq: 1,
    last_seq: 2,
    est_tokens: 11_000,
    status: "sealed",
    replacement: null,
    replacement_est_tokens: null,
    attempts: 0,
    interruptions: 0,
    last_error: null,
    claimed_generation: null,
    claimed_at: null,
    not_before: null,
    created_at: new Date().toISOString(),
    prepared_at: null,
    applied_at: null,
  };
  state.memoryChunks.push(chunk);
  const snapshot: MemorySnapshot = {
    messages: [
      { role: "user", content: "以前の話" },
      { role: "assistant", content: "以前の返事" },
    ],
    tools: [],
    ranges: [
      { first_seq: 1, last_seq: 1, message_index: 0 },
      { first_seq: 2, last_seq: 2, message_index: 1 },
    ],
  };
  let branch = await state.claimMemoryBranch(persona, generation, snapshot);
  assert.ok(branch);
  const checkpoint = initialMemoryState(branch, DEFAULT_MEMORY_POLICY);
  checkpoint.status = "paused";
  checkpoint.model_calls = DEFAULT_MEMORY_POLICY.maxRounds;
  checkpoint.pause_reason = "configured_budget";
  checkpoint.issue = {
    code: "memory_budget_exhausted",
    message: "Memory preparation reached its configured execution budget.",
  };
  branch = await state.saveMemoryBranch(
    persona,
    generation,
    chunk.chunk_seq,
    branch.revision,
    checkpoint,
  );
  await state.releaseWriter(persona, "preparation", generation);

  const requests: ModelRequest[] = [];
  const provider: ModelProvider = {
    name: "notice-only-recovery",
    async *stream(request) {
      requests.push(structuredClone(request));
      if (requests.length === 1) {
        // Derive the operation from the incoming notice, without fixture state
        // or a hardcoded sequence. The existing current-state block is also
        // asserted below: it is a separate, present-tense view of this branch.
        const notice = request.messages.at(-1)?.content;
        assert.ok(notice);
        assert.match(
          notice,
          /transition=occurred code=memory_budget_exhausted/,
        );
        const target = /chunk_seq=(\d+)/.exec(notice);
        assert.ok(target, "the delivered event identifies its own branch");
        yield {
          type: "tool_call",
          call: {
            id: "resume-from-notice",
            name: "memory.resume",
            route: "normal",
            arguments: {
              chunk_seq: Number(target[1]),
              mode: "resume",
              additional_rounds: 4,
            },
          },
        };
        yield { type: "done", usage: { finish_reason: "tool_calls" } };
      } else {
        yield { type: "done", usage: { finish_reason: "stop" } };
      }
    },
  };
  const life = secretary(state, provider);
  await life.start();
  try {
    assert.equal(await life.step({ startMemory: false }), "turn");
  } finally {
    await life.stop();
  }
  assert.equal(requests.length, 2);
  assert.ok(
    requests[0]?.messages.some(
      (m) =>
        m.content.startsWith("[Current memory mechanism status") &&
        m.content.includes('"chunk_seq":7319'),
    ),
    "current status already exposes its target independently of this notice",
  );
  const resumed = state.memoryBranches.get(`${persona}|${chunk.chunk_seq}`);
  assert.ok(resumed?.state);
  assert.equal(resumed.state.budget_extension?.rounds, 4);
  assert.deepEqual(resumed.snapshot, snapshot);
  const events = await state.events(persona, 0);
  const received = events.find((e) => e.kind === "input_received");
  const live = requests[0]?.messages.at(-1);
  assert.ok(received);
  assert.ok(live);
  assert.deepEqual(eventMessage(received), live);
  assert.equal(received.payload.chunk_seq, chunk.chunk_seq);
  assert.equal(events.filter((e) => e.kind === "tool_call").length, 1);
  assert.equal(events.filter((e) => e.kind === "tool_result").length, 1);
  assert.equal(
    state.outboxEntries.filter((entry) => entry.kind === "secretary_message")
      .length,
    0,
    "operational turn completion does not author a message",
  );

  const recoveredGeneration = (
    await state.acquireWriter(persona, "continued-preparation", 60_000)
  ).generation;
  const progress = structuredClone(resumed.state);
  progress.status = "running";
  progress.pause_reason = null;
  progress.retry_at = null;
  progress.issue = null;
  await state.saveMemoryBranch(
    persona,
    recoveredGeneration,
    chunk.chunk_seq,
    resumed.revision,
    progress,
  );
  await state.releaseWriter(
    persona,
    "continued-preparation",
    recoveredGeneration,
  );

  const restarted = secretary(state, provider);
  await restarted.start();
  try {
    assert.equal(await restarted.step({ startMemory: false }), "turn");
    const recoveredRequest = requests.at(-1);
    assert.ok(recoveredRequest);
    const recovery = recoveredRequest.messages.at(-1)?.content;
    assert.ok(recovery);
    assert.match(
      recovery,
      /chunk_seq=7319 transition=recovered code=memory_budget_exhausted/,
    );
    assert.doesNotMatch(recovery, /reached its configured execution budget/);
    assert.ok(
      !recoveredRequest.messages.some((m) =>
        m.content.startsWith("[Current memory mechanism status"),
      ),
      "the resolved fault no longer has a current-status entry",
    );
    assert.ok(
      recoveredRequest.messages.some(
        (m) => m.role === live.role && m.content === live.content,
      ),
    );
    assert.equal(
      state.memoryBranches.get(`${persona}|${chunk.chunk_seq}`)?.state
        ?.budget_extension?.rounds,
      4,
      "replay does not grant work again",
    );
  } finally {
    await restarted.stop();
  }
});

test("occurrence and recovery keep their distinct recorded meanings", async () => {
  for (const transition of ["occurred", "recovered"]) {
    const input: Input = {
      persona_id: persona,
      input_id: `memory:7319:2:${transition}`,
      kind: "memory_status",
      actor_kind: "memory",
      actor_id: "",
      source_surface: "core_memory",
      thread_id: "",
      attention: "observe",
      status: "queued",
      claimed_generation: null,
      turn_id: null,
      done_at: null,
      not_before: null,
      waiting_since: null,
      waited_ms: 0,
      occurred_at: "2026-09-30T00:00:00Z",
      created_at: "2026-09-30T00:00:00Z",
      payload: {
        text:
          transition === "occurred"
            ? "Preparation paused."
            : "Preparation recovered.",
        chunk_seq: 7319,
        transition,
        code: "memory_checkpoint_rejected",
      },
    };
    const event = inputReceivedEvent(input, { attempt: 1 } as Turn);
    assert.deepEqual(
      eventMessage({
        ...event,
        persona_id: persona,
        turn_id: "t",
        seq: 1,
        created_at: input.created_at,
      }),
      assemble([], input).at(-1),
    );
    assert.equal(event.payload.transition, transition);
  }
});
