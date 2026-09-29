import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
} from "../src/provider.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";

// Incoming messages open with a receipt line (conversation-continuity
// tests); these assertions are about what follows it.

const BULK = "y".repeat(2000);
const noticeOf = (req: ModelRequest) =>
  req.messages.find((m) =>
    m.content.includes("Working-context capacity notice"),
  );
const PERSONA = "01930e00-0000-7000-8000-0000000000b2";

/** A provider scripted per call: call number → its event stream. */
class ScriptedProvider implements ModelProvider {
  readonly name = "scripted";
  requests: ModelRequest[] = [];
  script: (call: number, req: ModelRequest) => AsyncIterable<ModelEvent> = () =>
    answer("ok");

  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(req);
    yield* this.script(this.requests.length, req);
  }
}

async function* answer(text: string): AsyncIterable<ModelEvent> {
  yield { type: "text", delta: text };
  yield { type: "done", usage: {} };
}

async function* refuseContext(): AsyncIterable<ModelEvent> {
  // A refusal can land mid-stream: partial text before the error is
  // discarded on the retry, never recorded.
  yield { type: "text", delta: "partial " };
  throw new ModelError(
    "model request failed: 400 context_length_exceeded: maximum context length is 131072 tokens",
    { retryable: false, refusal: "context_length" },
  );
}

async function* failTransient(): AsyncIterable<ModelEvent> {
  yield { type: "text", delta: "partial " };
  throw new ModelError("model request failed: 429 rate limited", {
    retryable: true,
    retryAfterMs: 10,
  });
}

function cfg(state: FakeState, provider: ModelProvider): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: "recovery-test",
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

async function turn(s: Secretary, state: FakeState, id: string, text: string) {
  state.addInput(PERSONA, id, text);
  assert.equal(await s.step(), "turn");
}

test("capacity refusal preserves the full context without a reduced-view retry", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  await turn(s, state, "one", "remember this original");
  provider.requests = [];
  provider.script = () => refuseContext();
  await turn(s, state, "two", "next request");
  assert.equal(provider.requests.length, 1);
  assert.match(
    JSON.stringify(provider.requests[0]!.messages),
    /remember this original/,
  );
  const last = Array.from(state.turns.values()).at(-1)!;
  assert.equal(last.status, "failed");
  assert.match(last.error!, /preserved without deleting records/);
  await s.stop();
});
test("recovery: a transient provider failure keeps its own retry semantics", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  provider.script = () => failTransient();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  for (let i = 0; i < 4; i++) {
    provider.script = () => answer("ok");
    await turn(s, state, `old-${i}`, `old-${i} ${BULK}`);
  }
  provider.script = () => failTransient();
  await turn(s, state, "live", "later");
  // One send, a retryable failure — no working-view recovery, no capacity
  // notice, and the input is requeued with durable pacing.
  assert.equal(provider.requests.length, 5);
  assert.ok(!noticeOf(provider.requests[4]!));
  const input = state.inputs.find((i) => i.input_id === "live")!;
  assert.equal(input.status, "queued");
  assert.ok(input.not_before, "retry is paced");
  await s.stop();
});

test("memory: the fake seal walk keeps an oversized committed turn whole", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const lease = await state.acquireWriter(PERSONA, "h", 30_000);
  const big = "x".repeat(41_000);
  const evs: { kind: string; payload: Record<string, unknown> }[] = [
    {
      kind: "input_received",
      payload: {
        input_id: "a",
        kind: "message",
        payload: { text: "hi" },
        actor_kind: "human",
      },
    },
    { kind: "assistant_message", payload: { text: big } },
    { kind: "tool_call", payload: { call_id: "c1", tool: "t", request: {} } },
    {
      kind: "tool_result",
      payload: { call_id: "c1", tool: "t", response: {} },
    },
    { kind: "assistant_message", payload: { text: big } },
    { kind: "tool_call", payload: { call_id: "c2", tool: "t", request: {} } },
    {
      kind: "tool_result",
      payload: { call_id: "c2", tool: "t", response: {} },
    },
    { kind: "assistant_message", payload: { text: "done" } },
    {
      kind: "input_received",
      payload: {
        input_id: "b",
        kind: "message",
        payload: { text: "hi" },
        actor_kind: "human",
      },
    },
    { kind: "assistant_message", payload: { text: "hi" } },
  ];
  evs.forEach((e, i) => {
    state.eventLog.push({
      persona_id: PERSONA,
      seq: i + 1,
      turn_id: "t",
      kind: e.kind,
      payload: e.payload,
      created_at: "2026-09-15T00:00:00.000Z",
    });
  });
  await state.memoryMaintain(PERSONA, lease.generation);
  // Same as the Go walk: no interior boundary exists inside the ~31k turn —
  // every continuation assistant follows a tool_result and a cut before a
  // tool_call would split deciding text from its effects. The whole turn
  // seals at the next input.
  assert.equal(state.memoryChunks.length, 1);
  const c = state.memoryChunks[0]!;
  assert.equal(c.first_seq, 1);
  assert.equal(c.last_seq, 8);
  assert.equal(c.status, "sealed");
  assert.ok(c.est_tokens > 20_000, "the indivisible turn exceeds the target");
  // And an indivisible flow seals whole past the limit — never split.
  const persona2 = "01930e00-0000-7000-8000-0000000000b3";
  state.addPersona(persona2);
  const lease2 = await state.acquireWriter(persona2, "h", 30_000);
  const flow: { kind: string; payload: Record<string, unknown> }[] = [
    {
      kind: "input_received",
      payload: {
        input_id: "a",
        kind: "message",
        payload: { text: "hi" },
        actor_kind: "human",
      },
    },
    { kind: "assistant_message", payload: { text: big } },
    { kind: "tool_call", payload: { call_id: "c1", tool: "t", request: {} } },
    {
      kind: "tool_result",
      payload: { call_id: "c1", tool: "t", response: {} },
    },
    { kind: "assistant_message", payload: { text: big } },
    {
      kind: "input_received",
      payload: {
        input_id: "b",
        kind: "message",
        payload: { text: "hi" },
        actor_kind: "human",
      },
    },
    { kind: "assistant_message", payload: { text: "hi" } },
  ];
  flow.forEach((e, i) => {
    state.eventLog.push({
      persona_id: persona2,
      seq: i + 1,
      turn_id: "t",
      kind: e.kind,
      payload: e.payload,
      created_at: "2026-09-15T00:00:00.000Z",
    });
  });
  await state.memoryMaintain(persona2, lease2.generation);
  const c2 = state.memoryChunks.find((x) => x.persona_id === persona2)!;
  assert.equal(c2.last_seq, 5);
  assert.ok(c2.est_tokens > 20_000, "the indivisible flow exceeds the target");
});
