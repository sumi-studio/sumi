import assert from "node:assert/strict";
import { test } from "node:test";
import { Secretary } from "../src/secretary.ts";
import { FakeState } from "../src/fake-state.ts";
import {
  advanceMemoryBranch,
  initialMemoryState,
  DEFAULT_MEMORY_POLICY,
  MEMORY_CHECKS,
  memoryPaths,
  memorySource,
  type MemoryBranch,
  type MemoryDecision,
  type MemorySnapshot,
} from "../src/memory-branch.ts";
import { runMemoryPreparation } from "../src/memory-compaction.ts";
import {
  ModelError,
  type ModelProvider,
  type ModelRequest,
  type ToolCall,
} from "../src/provider.ts";
import { StateError, FencedError } from "../src/state-client.ts";
import type { MemoryChunk } from "../src/types.ts";
const p = "01930e00-0000-7000-8000-0000000000a1";
function chunk(): MemoryChunk {
  return {
    persona_id: p,
    chunk_seq: 1,
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
    created_at: "2026-09-29T00:00:00Z",
    prepared_at: null,
    applied_at: null,
  };
}
function snapshot(): MemorySnapshot {
  return {
    messages: [
      { role: "system", content: "same system" },
      { role: "user", content: "ミナ: 応募はまだ決めていない。" },
      { role: "assistant", content: "一緒に考えよう。" },
      { role: "user", content: "the current main input" },
    ],
    tools: [
      {
        name: "file.write",
        description: "write",
        parameters: {
          type: "object",
          properties: { z: { type: "string" }, a: { type: "integer" } },
        },
      },
      {
        name: "file.read",
        description: "read",
        parameters: {
          type: "object",
          properties: { path: { type: "string" } },
        },
      },
    ],
    ranges: [
      { first_seq: 1, last_seq: 1, message_index: 1 },
      { first_seq: 2, last_seq: 2, message_index: 2 },
    ],
    binding: {
      version: 1,
      provider: "openai-responses",
      model: "gpt-6-astra",
      fingerprint: "fixed",
    },
  };
}
function branch(): MemoryBranch {
  const b: MemoryBranch = {
    chunk: chunk(),
    snapshot: snapshot(),
    state: null,
    revision: 0,
  };
  b.state = initialMemoryState(b, DEFAULT_MEMORY_POLICY);
  return b;
}
function call(
  name: string,
  args: Record<string, unknown>,
  route: "normal" | "elevated" = "normal",
): ToolCall {
  return { id: crypto.randomUUID(), route, name, arguments: args };
}
const decision = (text = "", calls: ToolCall[] = []): MemoryDecision => ({
  text,
  calls,
  usage: { input_tokens: 100, output_tokens: 10 },
});
async function step(b: MemoryBranch, text = "", calls: ToolCall[] = []) {
  b.state = await advanceMemoryBranch(b, decision(text, calls));
  return b.state;
}
async function draft(b: MemoryBranch, text = "ミナは応募をまだ決めていない。") {
  const path = memoryPaths(1);
  await step(b, "", [
    call("file.write", {
      path: path.candidate,
      content_text: text,
      expect_version: b.state?.candidate?.version ?? "none",
    }),
  ]);
}
async function read(b: MemoryBranch) {
  const path = memoryPaths(1);
  await step(b, "", [
    call("file.read", { path: path.source }),
    call("file.read", { path: path.candidate }),
  ]);
}
async function review(b: MemoryBranch) {
  await step(
    b,
    JSON.stringify({
      action: "review",
      version: b.state!.candidate!.version,
      sha256: b.state!.candidate!.sha256,
    }),
  );
}
function confirmation(b: MemoryBranch) {
  return JSON.stringify({
    action: "confirm",
    version: b.state!.review!.version,
    sha256: b.state!.review!.sha256,
    token: b.state!.review!.token,
    checks: Object.fromEntries(MEMORY_CHECKS.map((k) => [k, true])),
  });
}

test("private workspace denies other tools, routes, paths and source writes", async () => {
  const b = branch(),
    path = memoryPaths(1);
  const calls = [
    call("terminal.open", {}),
    call("conversation_history", { operation: "read" }),
    call("file.read", { path: "/etc/passwd" }),
    call("file.read", { path: path.source }, "elevated"),
    call("file.write", { path: path.source, content_text: "poison" }),
  ];
  await step(b, "", calls);
  const results = b
    .state!.messages.filter((m) => m.role === "tool")
    .map((m) => JSON.parse(m.content).error);
  assert.deepEqual(results, [
    "memory_branch_permission_denied",
    "memory_branch_permission_denied",
    "memory_branch_path_denied",
    "memory_branch_permission_denied",
    "memory_source_is_read_only",
  ]);
  assert.equal(b.state!.candidate, null);
  assert.ok(!memorySource(b).includes("the current main input")); // adjacent context is not injected into target
});
test("every edited candidate requires complete source and candidate reread plus a fresh modal", async () => {
  const b = branch();
  await draft(b);
  await review(b);
  assert.equal(b.state!.review, null);
  await read(b);
  await review(b);
  const old = confirmation(b);
  const token = b.state!.review!.token;
  await draft(b, "ミナは応募をまだ決めず、Sumiと考えている。");
  await step(b, old);
  assert.equal(b.state!.status, "running");
  assert.equal(b.state!.review, null);
  await read(b);
  await review(b);
  assert.notEqual(b.state!.review!.token, token);
  await step(b, confirmation(b));
  assert.equal(b.state!.status, "prepared");
  assert.equal(b.state!.final!.version, 2);
});
test("reread coverage is bytes, paginated UTF-8 survives, and stale file writes do not change a draft", async () => {
  const b = branch(),
    path = memoryPaths(1);
  await draft(b, "雨の午後。");
  await step(b, "", [
    call("file.write", {
      path: path.candidate,
      content_text: "overwrite",
      expect_version: "none",
    }),
  ]);
  assert.equal(b.state!.candidate!.text, "雨の午後。");
  await step(b, "", [
    call("file.read", { path: path.candidate, offset: 1, len: 10 }),
  ]);
  assert.match(b.state!.messages.at(-1)!.content, /offset_not_utf8_boundary/);
  await step(b, "", [
    call("file.read", { path: path.source }),
    call("file.read", { path: path.candidate, offset: 0, len: 6 }),
  ]);
  await review(b);
  assert.equal(b.state!.review, null);
  await step(b, "", [
    call("file.read", { path: path.candidate, offset: 6, len: 20 }),
  ]);
  await review(b);
  assert.ok(b.state!.review);
});
test("non-shrinking draft is normal review feedback, not a completion mechanism fault", async () => {
  const b = branch();
  b.chunk.est_tokens = 1;
  await draft(b);
  await read(b);
  await review(b);
  assert.equal(b.state!.status, "running");
  assert.equal(b.state!.issue, null);
  assert.match(b.state!.messages.at(-1)!.content, /not smaller/);
});
async function fixture() {
  const state = new FakeState();
  state.addPersona(p);
  const g = (await state.acquireWriter(p, "test", 60000)).generation;
  state.memoryChunks.push(chunk());
  return { state, g };
}
function scripted(state: FakeState, requests: ModelRequest[]): ModelProvider {
  return {
    name: "scripted",
    async *stream(req) {
      requests.push(req);
      const b = state.memoryBranches.get(`${p}|1`)!;
      const paths = memoryPaths(1);
      const st = b.state!;
      let d: MemoryDecision;
      if (!st.candidate)
        d = decision("", [
          call("file.write", {
            path: paths.candidate,
            content_text: "ミナの応募は未定。",
            expect_version: "none",
          }),
        ]);
      else if (st.source_read.length === 0)
        d = decision("", [
          call("file.read", { path: paths.source }),
          call("file.read", { path: paths.candidate }),
        ]);
      else if (!st.review)
        d = decision(
          JSON.stringify({
            action: "review",
            version: st.candidate.version,
            sha256: st.candidate.sha256,
          }),
        );
      else d = decision(confirmation(b));
      if (d.text) yield { type: "text", delta: d.text };
      for (const c of d.calls) yield { type: "tool_call", call: c };
      yield {
        type: "done",
        usage: {
          ...d.usage,
          finish_reason: d.calls.length ? "tool_calls" : "stop",
        },
      };
    },
  };
}
test("runner resumes JSON checkpoints with exact original prefix, pinned binding and medium suffix boundary", async () => {
  const { state, g } = await fixture();
  const requests: ModelRequest[] = [];
  const snap = snapshot();
  let calls = 0;
  const underlying = scripted(state, requests);
  const interrupted: ModelProvider = {
    name: "interrupt",
    async *stream(req) {
      if (calls++ === 1) throw new Error("network interrupted");
      yield* underlying.stream(req);
    },
  };
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: interrupted,
    snapshot: snap,
  });
  let b = state.memoryBranches.get(`${p}|1`)!;
  assert.equal(b.state!.candidate?.version, 1);
  assert.equal(b.state!.status, "paused");
  // Simulate durable JSON restore; draft/reads/progress, not an in-memory closure.
  b = JSON.parse(JSON.stringify(b));
  state.memoryBranches.set(`${p}|1`, b);
  b.state!.retry_at = new Date(0).toISOString();
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: underlying,
  });
  b = state.memoryBranches.get(`${p}|1`)!;
  assert.equal(b.state!.status, "prepared");
  assert.equal(b.state!.candidate!.version, 1);
  for (const req of requests) {
    assert.equal(
      JSON.stringify(req.messages.slice(0, snap.messages.length)),
      JSON.stringify(snap.messages),
    );
    assert.equal(JSON.stringify(req.tools), JSON.stringify(snap.tools));
    assert.deepEqual(req.bindingSnapshot, snap.binding);
    assert.equal(req.reasoningEffortAfter, snap.messages.length);
    assert.equal(req.reasoningEffort, "medium");
  }
});
test("lost final checkpoint receipt replays same version without another model call", async () => {
  const { state, g } = await fixture();
  const requests: ModelRequest[] = [];
  const save = state.saveMemoryBranch.bind(state);
  let lost = false;
  state.saveMemoryBranch = async (...args) => {
    const b = await save(...args);
    if (args[4].status === "prepared" && !lost) {
      lost = true;
      throw new StateError(503, "lost receipt");
    }
    return b;
  };
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: scripted(state, requests),
    snapshot: snapshot(),
  });
  assert.equal(requests.length, 4);
  assert.equal(state.memoryBranches.get(`${p}|1`)!.state!.status, "prepared");
  assert.equal(lost, true);
});
test("context refusal stops identical request, preserves original and draft, and emits one internal issue", async () => {
  const { state, g } = await fixture();
  let count = 0;
  const provider: ModelProvider = {
    name: "capacity",
    async *stream() {
      count++;
      throw new ModelError("too large", {
        retryable: true,
        unavailable: true,
        refusal: "context_length",
      });
    },
  };
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider,
    snapshot: snapshot(),
  });
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider,
    snapshot: snapshot(),
  });
  assert.equal(count, 1);
  const b = state.memoryBranches.get(`${p}|1`)!;
  assert.equal(b.state!.pause_reason, "context_capacity");
  assert.deepEqual(b.snapshot, snapshot());
  assert.equal(
    state.inputs.filter((i) => i.kind === "memory_status").length,
    1,
  );
  assert.equal(state.inputs[0]!.actor_kind, "memory");
  assert.equal(state.inputs[0]!.attention, "observe");
});
test("configured round stop keeps draft and changed limit resumes; model/semantic editing is not replayed", async () => {
  const { state, g } = await fixture();
  const reqs: ModelRequest[] = [];
  const provider = scripted(state, reqs);
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider,
    snapshot: snapshot(),
    policy: { maxRounds: 1 },
  });
  assert.equal(
    state.memoryBranches.get(`${p}|1`)!.state!.candidate!.version,
    1,
  );
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider,
    policy: { maxRounds: 8 },
  });
  assert.equal(state.memoryBranches.get(`${p}|1`)!.state!.status, "prepared");
  assert.equal(reqs.length, 4);
});
test("expired writer cannot checkpoint private effects", async () => {
  const { state, g } = await fixture();
  const b = (await state.claimMemoryBranch(p, g, snapshot()))!;
  state.leases.get(p)!.expires_at = new Date(0).toISOString();
  await assert.rejects(
    state.saveMemoryBranch(
      p,
      g,
      1,
      0,
      initialMemoryState(b, DEFAULT_MEMORY_POLICY),
    ),
    FencedError,
  );
});

test("changing the selected binding preserves the draft, invalidates review and rereads under the new pinned connection", async () => {
  const { state, g } = await fixture();
  const reqs: ModelRequest[] = [];
  const base = scripted(state, reqs);
  const changed = {
    ...snapshot().binding!,
    fingerprint: "new-connection",
    model: "gpt-5.6-terra",
  };
  let moved = false;
  const provider: ModelProvider = {
    name: "selection",
    snapshotBinding: async () => changed,
    async *stream(req) {
      if (req.round === 2 && !moved) {
        moved = true;
        throw new ModelError("selection changed", {
          retryable: false,
          unavailable: true,
          cause: "model_binding_changed",
        });
      }
      if (moved) {
        assert.deepEqual(req.bindingSnapshot, changed);
        assert.equal(req.reasoningEffort, undefined);
      }
      yield* base.stream(req);
    },
  };
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider,
    snapshot: snapshot(),
  });
  const b = state.memoryBranches.get(`${p}|1`)!;
  assert.equal(b.state!.status, "prepared");
  assert.equal(b.state!.candidate!.version, 1);
  assert.equal(b.state!.binding_changes!.length, 1);
  assert.equal(b.snapshot.binding!.fingerprint, "fixed");
  assert.equal(b.state!.effective_binding!.fingerprint, "new-connection");
  assert.ok(
    reqs.filter((r) =>
      r.messages.some(
        (m) => m.role === "tool" && m.content.includes("source.json"),
      ),
    ).length > 1,
  );
});
test("repeated interruption before a checkpoint becomes one durable mechanism issue, without discarding the last draft", async () => {
  const { state, g } = await fixture();
  let b = (await state.claimMemoryBranch(p, g, snapshot()))!;
  let st = initialMemoryState(b, DEFAULT_MEMORY_POLICY);
  st.in_flight = { round: 0, started_at: new Date().toISOString() };
  st.interruptions = 2;
  st = await advanceMemoryBranch(
    { ...b, state: st },
    decision("", [
      call("file.write", {
        path: memoryPaths(1).candidate,
        content_text: "saved before outage",
        expect_version: "none",
      }),
    ]),
  );
  b = await state.saveMemoryBranch(p, g, 1, b.revision, st);
  let calls = 0;
  const provider: ModelProvider = {
    name: "must not be consulted",
    async *stream() {
      calls++;
      throw new Error("unexpected");
    },
  };
  await runMemoryPreparation({ personaId: p, generation: g, state, provider });
  assert.equal(calls, 0);
  assert.equal(
    state.memoryBranches.get(`${p}|1`)!.state!.issue!.code,
    "memory_round_interrupted",
  );
  assert.equal(
    state.memoryBranches.get(`${p}|1`)!.state!.candidate!.text,
    "saved before outage",
  );
  assert.equal(
    state.inputs.filter((i) => i.kind === "memory_status").length,
    1,
  );
});

test("Japanese and emoji draft shrinking uses UTF-8 bytes, matching the database decision", async () => {
  for (const text of ["雨".repeat(12), "😀".repeat(9)]) {
    const b = branch();
    b.chunk.est_tokens = 9;
    await draft(b, text);
    await read(b);
    await review(b);
    assert.equal(new TextEncoder().encode(text).length, 36);
    assert.equal(b.state!.review, null);
    assert.match(b.state!.messages.at(-1)!.content, /not smaller/);
    await draft(b, text.slice(0, text === "雨".repeat(12) ? 8 : 12));
    await read(b);
    await review(b);
    assert.ok(b.state!.review);
    await step(b, confirmation(b));
    assert.equal(b.state!.status, "prepared");
  }
});
test("missing frozen private tool definitions pauses before any model call", async () => {
  const { state, g } = await fixture();
  const snap = snapshot();
  snap.tools = [];
  let calls = 0;
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    snapshot: snap,
    provider: {
      name: "unreachable",
      async *stream() {
        calls++;
        throw new Error("unexpected");
      },
    },
  });
  assert.equal(calls, 0);
  assert.equal(
    state.memoryBranches.get(`${p}|1`)!.state!.issue!.code,
    "memory_private_tools_missing",
  );
});

test("a final native call without another parent consultation is not reused as an idle branch prefix", async () => {
  const state = new FakeState();
  state.addPersona(p);
  let sent = 0;
  const provider: ModelProvider = {
    name: "pending boundary",
    async *stream() {
      if (sent++ === 0) yield { type: "text", delta: "prior" };
      else
        yield {
          type: "tool_call",
          call: call("journal.note", { text: "written" }),
        };
      yield {
        type: "done",
        usage: { finish_reason: sent === 1 ? "stop" : "tool_calls" },
      };
    },
  };
  const secretary = new Secretary({
    personaId: p,
    holderId: "test",
    state,
    provider,
    maxToolRounds: 1,
    leaseTtlMs: 60000,
    renewEveryMs: 10000,
    contextLimit: 5000,
    pollIntervalMs: 1,
    scheduleEveryMs: 60000,
    idgen: () => crypto.randomUUID(),
  });
  await secretary.start();
  try {
    state.addInput(p, "first", "hello");
    await secretary.step({ startMemory: false });
    const last = (await state.events(p, 0)).at(-1)!.seq;
    state.addInput(p, "second", "note");
    await secretary.step({ startMemory: false });
    assert.ok((await state.events(p, 0)).some((e) => e.kind === "note"));
    state.memoryChunks.push({ ...chunk(), last_seq: last });
    await secretary.step({ startMemory: false });
    assert.equal(state.memoryBranches.size, 0);
  } finally {
    await secretary.stop();
  }
});

async function requestResume(
  state: FakeState,
  g: number,
  mode: "resume" | "rebranch",
  rounds = 8,
) {
  const input = `resume-${crypto.randomUUID()}`,
    turn = `turn-${input}`;
  state.addInput(p, input, "resume memory");
  await state.loadTurn(p, g, turn, 5000);
  const request = { chunk_seq: 1, mode, additional_rounds: rounds };
  await state.savePlan(p, g, {
    turnId: turn,
    round: 0,
    text: "",
    calls: [
      {
        call_id: "resume-call",
        tool: "memory.resume",
        route: "normal",
        request,
      },
    ],
    usage: {},
  });
  const op = {
    operationId: `${turn}:op:0`,
    turnId: turn,
    tool: "memory.resume",
    callIndex: 0,
    request,
  };
  const first = await state.claimOperation(p, g, op);
  const revision = state.memoryBranches.get(`${p}|1`)!.revision;
  const replay = await state.claimOperation(p, g, op);
  assert.deepEqual(replay.operation.response, first.operation.response);
  assert.equal(
    state.memoryBranches.get(`${p}|1`)!.revision,
    revision,
    "replayed operation must not grant budget twice",
  );
}
test("same-binding recovery is paced, counts failed admissions and stops at the finite budget", async () => {
  const { state, g } = await fixture();
  let calls = 0;
  const provider: ModelProvider = {
    name: "outage",
    async *stream() {
      calls++;
      throw new Error("broken transport");
    },
  };
  for (let i = 0; i < 3; i++) {
    await runMemoryPreparation({
      personaId: p,
      generation: g,
      state,
      provider,
      snapshot: snapshot(),
      policy: { maxRounds: 3, maxConsecutiveFailures: 2 },
    });
    const b = state.memoryBranches.get(`${p}|1`)!;
    assert.ok(b.state!.retry_at);
    await runMemoryPreparation({
      personaId: p,
      generation: g,
      state,
      provider,
      snapshot: snapshot(),
      policy: { maxRounds: 3, maxConsecutiveFailures: 2 },
    });
    assert.equal(calls, i + 1, "backoff must not issue a request");
    b.state!.retry_at = new Date(0).toISOString();
  }
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider,
    snapshot: snapshot(),
    policy: { maxRounds: 3, maxConsecutiveFailures: 2 },
  });
  const b = state.memoryBranches.get(`${p}|1`)!;
  assert.equal(b.state!.pause_reason, "configured_budget");
  assert.equal(b.state!.model_calls, 3);
  assert.equal(calls, 3);
  await requestResume(state, g, "resume", 8);
  const requests: ModelRequest[] = [];
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: scripted(state, requests),
    policy: { maxRounds: 3, maxConsecutiveFailures: 2 },
  });
  assert.equal(state.memoryBranches.get(`${p}|1`)!.state!.status, "prepared");
  assert.deepEqual(state.memoryBranches.get(`${p}|1`)!.snapshot, snapshot());
  assert.equal(
    state.inputs.filter(
      (i) =>
        i.kind === "memory_status" &&
        i.payload.text
          ?.toString()
          .includes("mechanism occurred] The memory branch repeatedly failed"),
    ).length,
    1,
  );
});
test("explicit rebranch waits for a real matching parent source, archives the old attempt and rereviews its draft", async () => {
  const { state, g } = await fixture();
  const original = snapshot();
  original.tools = [];
  let b = (await state.claimMemoryBranch(p, g, original))!;
  let st = initialMemoryState(b, {
    ...DEFAULT_MEMORY_POLICY,
    maxTokens: 10000,
  });
  st = await advanceMemoryBranch(
    { ...b, state: st },
    decision("", [
      call("file.write", {
        path: memoryPaths(1).candidate,
        content_text: "ミナの応募は未定。",
        expect_version: "none",
      }),
    ]),
  );
  b = await state.saveMemoryBranch(p, g, 1, b.revision, st);
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: scripted(state, []),
  });
  await requestResume(state, g, "rebranch", 5);
  assert.equal(await state.claimMemoryBranch(p, g), null);
  const wrong = snapshot();
  wrong.ranges[0]!.chunk_seq = 999;
  wrong.ranges[0]!.layer = 1;
  assert.equal(
    await state.claimMemoryBranch(p, g, wrong),
    null,
    "same seq with another source cannot restart",
  );
  assert.equal(
    await state.claimMemoryBranch(p, g, original),
    null,
    "still missing the frozen private tools",
  );
  const requests: ModelRequest[] = [];
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: scripted(state, requests),
    snapshot: snapshot(),
  });
  b = state.memoryBranches.get(`${p}|1`)!;
  assert.equal(b.state!.status, "prepared");
  assert.equal(b.state!.candidate!.version, 1);
  assert.equal(b.state!.execution_budget!.rounds, 5);
  assert.equal(
    b.state!.execution_budget!.tokens,
    10000 - st.tokens,
    "rebranch retains the remaining token cap",
  );
  assert.equal(
    requests.length,
    3,
    "reuse draft but reread, review and confirm anew",
  );
  assert.match(requests[0]!.messages.at(-1)!.content, /new attempt/);
  assert.equal(state.memoryBranchAttempts.get(`${p}|1`)!.length, 1);
  assert.deepEqual(
    state.memoryBranchAttempts.get(`${p}|1`)![0]!.snapshot,
    original,
  );
});

for (const outcome of ["prepared", "kept"] as const) {
  test(`a rejected ${outcome} checkpoint recovers only when final storage succeeds, not on a private read`, async () => {
    const { state, g } = await fixture();
    let b = (await state.claimMemoryBranch(p, g, snapshot()))!;
    b.state = initialMemoryState(b, DEFAULT_MEMORY_POLICY);
    if (outcome === "prepared") await draft(b);
    await read(b);
    if (outcome === "prepared") await review(b);
    else await step(b, JSON.stringify({ action: "review_keep" }));
    b = await state.saveMemoryBranch(p, g, 1, b.revision, b.state!);
    const save = state.saveMemoryBranch.bind(state);
    let rejectFinal = true,
      rejectCount = 0,
      readFirst = false;
    const durableReads: string[] = [];
    state.saveMemoryBranch = async (...args) => {
      if (args[4].status === outcome && rejectFinal) {
        rejectCount++;
        throw new StateError(400, "persistent final-store fault");
      }
      const previousRound = state.memoryBranches.get(`${p}|1`)!.state!.rounds;
      const saved = await save(...args);
      if (
        args[4].rounds > previousRound &&
        args[4].messages.at(-1)?.role === "tool"
      )
        durableReads.push(args[4].issue?.code ?? "none");
      return saved;
    };
    const provider: ModelProvider = {
      name: "retry reads before confirming",
      async *stream() {
        if (readFirst) {
          readFirst = false;
          yield {
            type: "tool_call",
            call: call("file.read", { path: memoryPaths(1).source }),
          };
          yield { type: "done", usage: { finish_reason: "tool_calls" } };
        } else {
          yield {
            type: "text",
            delta: confirmation(state.memoryBranches.get(`${p}|1`)!),
          };
          yield { type: "done", usage: { finish_reason: "stop" } };
        }
      },
    };
    const notices = () =>
      state.inputs
        .filter((i) => i.kind === "memory_status")
        .map(
          (i) =>
            String(i.payload.text).match(/mechanism (occurred|recovered)/)![1],
        );
    await runMemoryPreparation({
      personaId: p,
      generation: g,
      state,
      provider,
    });
    assert.deepEqual(notices(), ["occurred"]);
    state.memoryBranches.get(`${p}|1`)!.state!.retry_at = new Date(
      0,
    ).toISOString();
    readFirst = true;
    await runMemoryPreparation({
      personaId: p,
      generation: g,
      state,
      provider,
    });
    assert.equal(rejectCount, 2);
    assert.deepEqual(durableReads, ["memory_checkpoint_rejected"]);
    assert.deepEqual(
      notices(),
      ["occurred"],
      "an unrelated read must not recover and re-announce the same failure",
    );
    rejectFinal = false;
    readFirst = true;
    state.memoryBranches.get(`${p}|1`)!.state!.retry_at = new Date(
      0,
    ).toISOString();
    await runMemoryPreparation({
      personaId: p,
      generation: g,
      state,
      provider,
    });
    assert.deepEqual(durableReads, [
      "memory_checkpoint_rejected",
      "memory_checkpoint_rejected",
    ]);
    assert.equal(state.memoryBranches.get(`${p}|1`)!.state!.status, outcome);
    assert.equal(state.memoryBranches.get(`${p}|1`)!.state!.issue, null);
    assert.deepEqual(notices(), ["occurred", "recovered"]);
  });
}
test("a control protocol fault recovers on a successful review, after source reads retain the fault", async () => {
  const { state, g } = await fixture();
  let b = (await state.claimMemoryBranch(p, g, snapshot()))!;
  b.state = initialMemoryState(b, DEFAULT_MEMORY_POLICY);
  await draft(b);
  for (let i = 0; i < DEFAULT_MEMORY_POLICY.maxConsecutiveFailures; i++)
    await step(b, "not a control response");
  assert.equal(b.state!.issue!.code, "memory_control_protocol");
  b = await state.saveMemoryBranch(p, g, 1, b.revision, b.state!);
  state.memoryBranches.get(`${p}|1`)!.state!.retry_at = new Date(
    0,
  ).toISOString();
  const save = state.saveMemoryBranch.bind(state);
  const evidence: { role: string; issue: string | null; review: boolean }[] =
    [];
  state.saveMemoryBranch = async (...args) => {
    const saved = await save(...args);
    if (!args[4].in_flight)
      evidence.push({
        role: args[4].messages.at(-1)!.role,
        issue: args[4].issue?.code ?? null,
        review: !!args[4].review,
      });
    return saved;
  };
  await runMemoryPreparation({
    personaId: p,
    generation: g,
    state,
    provider: scripted(state, []),
  });
  assert.ok(
    evidence.some(
      (e) =>
        e.role === "tool" && e.issue === "memory_control_protocol" && !e.review,
    ),
  );
  assert.ok(evidence.some((e) => e.review && e.issue === null));
  assert.equal(state.memoryBranches.get(`${p}|1`)!.state!.status, "prepared");
  assert.equal(
    state.inputs.filter((i) => i.kind === "memory_status").length,
    2,
  );
});
