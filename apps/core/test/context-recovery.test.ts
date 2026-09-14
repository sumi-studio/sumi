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

const noticeOf = (req: ModelRequest) =>
  req.messages.find((m) => m.content.includes("Working-context capacity notice"));

test("recovery: a refused request retries on a reduced working view and completes", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  // Four completed exchanges = eight older journal records.
  for (let i = 0; i < 4; i++) {
    await turn(s, state, `old-${i}`, `old-${i}`);
  }
  provider.script = (call) => (call === 5 ? refuseContext() : answer("still here"));
  await turn(s, state, "live", "the live request");
  assert.equal(provider.requests.length, 6);

  const recovered = provider.requests[5]!;
  assert.equal(recovered.messages[0]!.role, "system");
  // The capacity notice names exactly the records left out and how to
  // reread them — the oldest four records, dropped whole.
  const notice = noticeOf(recovered);
  assert.ok(notice, "capacity notice present");
  assert.match(notice!.content, /journal seq 1 through 4/);
  assert.match(notice!.content, /conversation_history/);
  // The evicted text is gone from this send; the retained tail and the
  // current input are not.
  const bodies = recovered.messages.map((m) => m.content);
  assert.ok(!bodies.some((c) => c.includes("old-0")), "evicted records absent");
  assert.ok(bodies.some((c) => c.includes("old-2")), "retained tail present");
  assert.equal(recovered.messages.at(-1)!.content, "[human] the live request");
  assert.equal(recovered.messages.at(-1)!.role, "user");

  // The journal itself was never touched: every record — including the
  // evicted ones — is still there, and the turn completed once.
  const evs = await state.events(PERSONA, 0);
  assert.equal(evs.filter((e) => e.kind === "input_received").length, 5);
  const input = state.inputs.find((i) => i.input_id === "live")!;
  assert.equal(input.status, "done");
  await s.stop();
});

test("recovery: the in-turn assistant/tool suffix rides along; effects are not replayed", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  for (let i = 0; i < 4; i++) {
    await turn(s, state, `old-${i}`, `old-${i}`);
  }
  provider.script = (call) => {
    if (call === 5) {
      // Round 0 decides one internal tool call.
      return (async function* () {
        yield {
          type: "tool_call" as const,
          call: {
            id: "c1",
            name: "journal.note",
            arguments: { text: "remembered" },
          },
        };
        yield { type: "done" as const, usage: {} };
      })();
    }
    if (call === 6) return refuseContext(); // round 1 first attempt
    return answer("answer after the note");
  };
  await turn(s, state, "live", "please remember this");
  assert.equal(provider.requests.length, 7);

  const recovered = provider.requests[6]!;
  assert.ok(noticeOf(recovered), "capacity notice present");
  const suffix = recovered.messages.slice(-3);
  assert.equal(suffix[0]!.content, "[human] please remember this");
  assert.equal(suffix[1]!.role, "assistant");
  assert.equal(suffix[1]!.toolCalls?.length, 1);
  assert.equal(suffix[1]!.toolCalls![0]!.name, "journal.note");
  assert.equal(suffix[2]!.role, "tool");
  assert.equal(suffix[2]!.toolCallId, "c1");

  // The note's effect committed once — recovery never replays side effects.
  const evs = await state.events(PERSONA, 0);
  assert.equal(evs.filter((e) => e.kind === "note").length, 1);
  assert.equal(
    evs.filter((e) => e.kind === "tool_result" && e.payload.call_id === "c1")
      .length,
    1,
  );
  const input = state.inputs.find((i) => i.input_id === "live")!;
  assert.equal(input.status, "done");
  await s.stop();
});

test("recovery: persistent refusals end in a bounded, honest failure", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  provider.script = () => refuseContext();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  for (let i = 0; i < 4; i++) {
    provider.script = () => answer("ok");
    await turn(s, state, `old-${i}`, `old-${i}`);
  }
  provider.script = () => refuseContext();
  await turn(s, state, "live", "too big");
  // 1 initial + 2 recoveries = 3 sends, then a recorded failure.
  assert.equal(provider.requests.length, 7);
  // Each recovery evicts more of the oldest records.
  assert.match(noticeOf(provider.requests[5]!)!.content, /seq 1 through 4/);
  assert.match(noticeOf(provider.requests[6]!)!.content, /seq 1 through 6/);

  const input = state.inputs.find((i) => i.input_id === "live")!;
  assert.equal(input.status, "done", "a capacity failure is terminal, not requeued");
  const failed = state.outboxEntries.find((e) => e.kind === "turn_failed")!;
  assert.match(String(failed.payload.error), /refused the request for context size/);
  assert.match(String(failed.payload.error), /2 reduced working-view attempt/);
  await s.stop();
});

test("recovery: a refusal framed as retryable is still terminal", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  for (let i = 0; i < 4; i++) {
    provider.script = () => answer("ok");
    await turn(s, state, `old-${i}`, `old-${i}`);
  }
  // A provider can report a capacity refusal inside an otherwise
  // transient-looking error (e.g. an in-band stream error with a 5xx code).
  // The refusal still wins: bounded recoveries, then a terminal failure —
  // never a requeue onto the transient budget.
  provider.script = () =>
    (async function* () {
      yield { type: "text" as const, delta: "partial " };
      throw new ModelError(
        "provider stream error: context_length_exceeded",
        { retryable: true, refusal: "context_length" },
      );
    })();
  await turn(s, state, "live", "too big");
  assert.equal(provider.requests.length, 7); // 1 + 2 recoveries
  const input = state.inputs.find((i) => i.input_id === "live")!;
  assert.equal(input.status, "done", "capacity failure stays terminal");
  const failed = state.outboxEntries.find((e) => e.kind === "turn_failed")!;
  assert.match(String(failed.payload.error), /refused the request for context size/);
  await s.stop();
});

test("recovery: nothing reducible fails at once, without burning recoveries", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ScriptedProvider();
  provider.script = () => refuseContext();
  const s = new Secretary(cfg(state, provider));
  await s.start();
  // No prior journal records — only the protected suffix remains.
  await turn(s, state, "live", "unrecoverable");
  assert.equal(provider.requests.length, 1);
  const failed = state.outboxEntries.find((e) => e.kind === "turn_failed")!;
  assert.match(String(failed.payload.error), /no reducible records/);
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
    await turn(s, state, `old-${i}`, `old-${i}`);
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

test("memory: the fake seal walk cuts an oversized committed turn at a safe boundary", async () => {
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
    { kind: "tool_result", payload: { call_id: "c1", tool: "t", response: {} } },
    { kind: "assistant_message", payload: { text: big } },
    { kind: "tool_call", payload: { call_id: "c2", tool: "t", request: {} } },
    { kind: "tool_result", payload: { call_id: "c2", tool: "t", response: {} } },
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
  evs.forEach((e, i) =>
    state.eventLog.push({
      persona_id: PERSONA,
      seq: i + 1,
      turn_id: "t",
      kind: e.kind,
      payload: e.payload,
      created_at: "2026-09-15T00:00:00.000Z",
    }),
  );
  await state.memoryMaintain(PERSONA, lease.generation);
  // Same as the Go walk: the ~21k window cuts before the second tool_call
  // (seq 6) — inside the turn but never inside a call→result group.
  assert.equal(state.memoryChunks.length, 1);
  const c = state.memoryChunks[0]!;
  assert.equal(c.first_seq, 1);
  assert.equal(c.last_seq, 5);
  assert.equal(c.status, "sealed");
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
    { kind: "tool_result", payload: { call_id: "c1", tool: "t", response: {} } },
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
  flow.forEach((e, i) =>
    state.eventLog.push({
      persona_id: persona2,
      seq: i + 1,
      turn_id: "t",
      kind: e.kind,
      payload: e.payload,
      created_at: "2026-09-15T00:00:00.000Z",
    }),
  );
  await state.memoryMaintain(persona2, lease2.generation);
  const c2 = state.memoryChunks.find((x) => x.persona_id === persona2)!;
  assert.equal(c2.last_seq, 5);
  assert.ok(c2.est_tokens > 20_000, "the indivisible flow exceeds the target");
});
