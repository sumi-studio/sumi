import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  COMPACT_L1_PROMPT,
  renderJournalContext,
  runMemoryPreparation,
} from "../src/memory.ts";
import {
  type ChatMessage,
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
  type ToolSpec,
} from "../src/provider.ts";
import { AnthropicProvider } from "../src/providers/anthropic.ts";
import { continuationScope } from "../src/providers/chatgpt-codex.ts";
import { OpenAIProvider } from "../src/providers/openai.ts";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { type StateClient, StateError } from "../src/state-client.ts";
import type { Event, ToolRoute } from "../src/types.ts";
import { BudgetWaitError } from "../src/usage.ts";

/**
 * What the secretary did with tools stays part of what she remembers: a
 * Messaging send or edit, and what it actually returned, render in every
 * later context as the call/result pair the model produced and received —
 * after a restart, on another model, and in the memory-preparation branch
 * — and representing them never runs them again.
 */

const PERSONA = "01930e00-0000-7000-8000-0000000000f1";
const HUMAN = "01930e00-0000-7000-8000-0000000000f2";
const PLACE = "0190a8a0-0000-7000-8000-00000000c0de";
const SENT = "明日の打ち合わせは15:30からB会議室です。";
const EDITED = "訂正: 明日の打ち合わせは16:00からB会議室です。";
const RECEIPT = {
  message_id: "0190a8a0-0000-7000-8000-00000000beef",
  place_id: PLACE,
  seq: 3,
  created: true,
  reply_to: "",
  created_at: "2026-09-29T00:00:01Z",
};

type Round = {
  text?: string;
  calls?: {
    tool: string;
    route?: ToolRoute;
    request: Record<string, unknown>;
  }[];
};

/**
 * Scripted by input: each served input answers with its planned rounds
 * (call ids `call_<input>_<round>_<i>`, as a provider would mint them).
 * Every request is recorded. A memory branch keeps its target unchanged.
 */
class Script implements ModelProvider {
  readonly name: string;
  readonly requests: ModelRequest[] = [];
  private readonly plans: Record<string, Round[]>;
  constructor(name: string, plans: Record<string, Round[]>) {
    this.name = name;
    this.plans = plans;
  }
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(structuredClone({ ...req, signal: undefined }));
    if (req.phase === "memory") {
      yield { type: "text", delta: "KEEP_UNCHANGED" };
      yield { type: "done", usage: { finish_reason: "stop" } };
      return;
    }
    const id = req.inputId ?? "";
    const r = this.plans[id]?.[req.round] ?? { text: `reply to ${id}` };
    if (r.text) yield { type: "text", delta: r.text };
    for (const [i, c] of (r.calls ?? []).entries()) {
      yield {
        type: "tool_call",
        call: {
          id: `call_${id}_${req.round}_${i}`,
          name: c.tool,
          route: c.route ?? "normal",
          arguments: c.request,
        },
      };
    }
    yield { type: "done", usage: {} };
  }
}

function cfg(
  state: StateClient,
  provider: ModelProvider,
  over: Partial<SecretaryConfig> = {},
): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: `h-${crypto.randomUUID()}`,
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 200,
    pollIntervalMs: 1,
    scheduleEveryMs: 60_000,
    idgen: () => crypto.randomUUID(),
    ...over,
  };
}

/** Serve every queued input with `provider` on a fresh secretary. Memory
 * preparation is left to the tests that drive it explicitly. */
async function serve(
  state: StateClient,
  provider: ModelProvider,
  over: Partial<SecretaryConfig> = {},
): Promise<void> {
  const s = new Secretary(cfg(state, provider, over));
  await s.start();
  for (
    let i = 0;
    i < 20 && (await s.step({ startMemory: false })) !== "idle";
    i++
  );
  await s.stop();
}

/** A store with Messaging send/edit wired as delegated effects, counting
 * every time an effect actually runs. */
function messagingState(): {
  state: FakeState;
  sends: Record<string, unknown>[];
  edits: Record<string, unknown>[];
} {
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  const sends: Record<string, unknown>[] = [];
  const edits: Record<string, unknown>[] = [];
  state.registerEffect("messaging.send", (_p, _idem, req) => {
    sends.push(req);
    return RECEIPT;
  });
  state.registerEffect("messaging.edit_message", (_p, _idem, req) => {
    edits.push(req);
    return {
      message: {
        message_id: req.message_id,
        content: req.content,
        revision: 2,
        edited: true,
      },
      place_id: req.place_id,
      message_id: req.message_id,
      replayed: false,
    };
  });
  return { state, sends, edits };
}

const sendCall = {
  tool: "messaging.send",
  request: { place_id: PLACE, content: SENT },
};
const editCall = {
  tool: "messaging.edit_message",
  request: { place_id: PLACE, message_id: RECEIPT.message_id, content: EDITED },
};

/** Test clock step: a requeued input's retry backoff has elapsed. */
function requeueNow(state: FakeState, inputId: string): void {
  const input = state.inputs.find(
    (i) => i.persona_id === PERSONA && i.input_id === inputId,
  );
  assert.ok(input);
  input.not_before = null;
}

/** The journal portion of a turn request: between the system prompt and
 * the current input. */
const journalView = (req: ModelRequest): ChatMessage[] =>
  req.messages.slice(1, -1);

test("a sent and an edited Messaging message stay in the next context as the calls that made them, across a restart, without running again", async () => {
  const { state, sends, edits } = messagingState();
  state.addInput(PERSONA, "in-1", "Please tell the group the meeting time.");
  state.addInput(PERSONA, "in-2", "It moved to 16:00 — fix the post.");
  const a = new Script("model-a", {
    "in-1": [{ text: "Posting it now.", calls: [sendCall] }, { text: "Sent." }],
    "in-2": [{ calls: [editCall] }, { text: "Fixed." }],
  });
  await serve(state, a);
  assert.equal(sends.length, 1);
  assert.equal(edits.length, 1);

  // The host restarts on another model; the human asks what was posted.
  state.addInput(PERSONA, "in-3", "What exactly did you post?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const view = journalView(b.requests[0]!);

  const receiptOf = (m: ModelRequest) => m.messages.at(-1)!;
  // model-a's round-0 request for in-1 ends with in-1 as the current input.
  const in1 = receiptOf(a.requests[0]!);
  const in2 = receiptOf(a.requests[2]!);
  assert.deepEqual(view, [
    in1,
    {
      role: "assistant",
      content: "Posting it now.",
      toolCalls: [
        {
          id: "call_in-1_0_0",
          name: "messaging.send",
          route: "normal",
          arguments: { place_id: PLACE, content: SENT },
        },
      ],
    },
    {
      role: "tool",
      toolCallId: "call_in-1_0_0",
      name: "messaging.send",
      content: JSON.stringify(RECEIPT),
    },
    { role: "assistant", content: "Sent." },
    in2,
    {
      role: "assistant",
      content: "",
      toolCalls: [
        {
          id: "call_in-2_0_0",
          name: "messaging.edit_message",
          route: "normal",
          arguments: editCall.request,
        },
      ],
    },
    {
      role: "tool",
      toolCallId: "call_in-2_0_0",
      name: "messaging.edit_message",
      content: view[6]!.content,
    },
    { role: "assistant", content: "Fixed." },
  ]);
  // The edit's result is what the edit returned — the revised message.
  assert.match(view[6]!.content, /訂正: 明日の打ち合わせは16:00/);
  // The body the secretary wrote is hers (the call); the receipt stays a
  // tool result and is never presented as her speech.
  assert.ok(
    !view
      .filter((m) => m.role === "assistant")
      .some((m) => m.content.includes(RECEIPT.message_id)),
  );

  // Faithful across the restart: the later context carries exactly the
  // suffix model-a was fed live when it decided in-1's final reply.
  assert.deepEqual(a.requests[1]!.messages.slice(-3), view.slice(0, 3));

  // Representing the calls ran nothing: no new effect, no new operation.
  const ops = state.ops.size;
  state.addInput(PERSONA, "in-4", "Thanks.");
  await serve(state, new Script("model-c", {}));
  assert.equal(sends.length, 1, "the send ran exactly once");
  assert.equal(edits.length, 1, "the edit ran exactly once");
  assert.equal(state.ops.size, ops, "no operation was claimed for history");
});

test("a turn that fails after sending tells the next context the send happened", async () => {
  const { state, sends } = messagingState();
  state.addInput(PERSONA, "in-1", "Tell the group the meeting time.");
  // The model keeps calling tools past the round cap: the send commits,
  // then the turn fails non-retryably.
  await serve(
    state,
    new Script("model-a", {
      "in-1": [{ text: "Posting.", calls: [sendCall] }],
    }),
    { maxToolRounds: 1 },
  );
  assert.equal(sends.length, 1);
  // The separate requester-facing failure notice is unchanged.
  assert.ok(
    (await state.outbox(PERSONA, 0)).some((o) => o.kind === "turn_failed"),
  );

  state.addInput(PERSONA, "in-2", "Did it go out?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const view = journalView(b.requests[0]!);
  assert.deepEqual(
    view.map((m) => m.role),
    ["user", "assistant", "tool", "user"],
  );
  assert.equal(view[1]!.toolCalls?.[0]?.arguments.content, SENT);
  const marker = view[3]!.content;
  assert.ok(!marker.includes("without a completed reply"), marker);
  assert.match(
    marker,
    /^\[turn failed — this request stopped here without finishing normally\. /,
  );
  assert.match(marker, /a message you sent stays sent/);
  assert.equal(sends.length, 1, "nothing was sent again");
});

test("rejected, pending, denied and uncertain calls stay distinguishable in later context", async () => {
  const { state, sends } = messagingState();
  // A rejected call: the store refuses an unknown tool before any effect.
  state.addInput(PERSONA, "in-1", "Try the old tool.");
  // A gated call: parks for the human's decision.
  state.addInput(PERSONA, "in-2", "Announce it to everyone.");
  const a = new Script("model-a", {
    "in-1": [
      { calls: [{ tool: "no.such.tool", request: { x: 1 } }] },
      { text: "That tool is gone." },
    ],
    "in-2": [
      {
        text: "Asking first.",
        calls: [
          { tool: "message.send", route: "elevated", request: { text: SENT } },
        ],
      },
      { text: "It was not sent." },
    ],
  });
  await serve(state, a);

  // While the request waits, another conversation continues.
  state.addInput(PERSONA, "in-3", "Anything pending?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const waiting = journalView(b.requests[0]!);
  const rejected = waiting.findIndex((m) =>
    m.toolCalls?.some((c) => c.name === "no.such.tool"),
  );
  assert.ok(rejected > 0);
  assert.deepEqual(waiting[rejected + 1], {
    role: "tool",
    toolCallId: "call_in-1_0_0",
    name: "no.such.tool",
    content: JSON.stringify({ error: "unknown tool: no.such.tool" }),
  });
  const pendingAt = waiting.findIndex((m) =>
    m.content.startsWith("[Approval requested"),
  );
  const pendingNote = waiting[pendingAt];
  assert.ok(pendingNote);
  // What the waiting request already said is part of the record now.
  assert.deepEqual(waiting[pendingAt - 1], {
    role: "assistant",
    content: "Asking first.",
  });
  assert.equal(pendingNote.role, "user");
  assert.match(
    pendingNote.content,
    /your elevated call message\.send \(call_id call_in-2_0_0\) is waiting for a human decision \(approval_id [^)]+\); it has not run at this point\./,
  );
  assert.ok(pendingNote.content.includes(SENT));
  // A pending call is not a native call: no result exists yet.
  assert.ok(
    !waiting.some((m) => m.toolCalls?.some((c) => c.name === "message.send")),
  );

  // The human denies it; the parked request resumes and records the denial.
  const [appr] = (await state.listApprovals(PERSONA)).filter(
    (x) => x.status === "pending",
  );
  assert.ok(appr);
  await state.resolveApproval(PERSONA, appr.approval_id, {
    decision: "deny_once",
    decision_id: "d-1",
    decided_by_kind: "human",
    decided_by_id: HUMAN,
  });
  await serve(state, a);
  state.addInput(PERSONA, "in-4", "So what happened?");
  const c = new Script("model-c", {});
  await serve(state, c);
  const after = journalView(c.requests[0]!);
  assert.ok(
    after.some((m) =>
      m.content.startsWith(
        `[Approval decided: deny_once by human for your elevated call message.send (approval_id ${appr.approval_id}).`,
      ),
    ),
  );
  const denied = after.findIndex((m) =>
    m.toolCalls?.some((x) => x.name === "message.send"),
  );
  assert.ok(denied > 0);
  // The deciding text was journaled when the request parked; the resumed
  // attempt records only what happened after the decision.
  assert.equal(after[denied]!.content, "");
  assert.equal(
    after.filter((m) => m.content === "Asking first.").length,
    1,
    "the parked round's text appears once",
  );
  assert.equal(after[denied]!.toolCalls?.[0]?.route, "elevated");
  assert.deepEqual(after[denied + 1], {
    role: "tool",
    toolCallId: "call_in-2_0_0",
    name: "message.send",
    content: JSON.stringify({ error: "denied" }),
  });
  assert.equal(sends.length, 0);
});

test("a call left running by a prior attempt gets one stored unknown-outcome receipt that every attempt reads", async () => {
  const { state, sends } = messagingState();
  state.addInput(PERSONA, "in-1", "post it");
  // An earlier attempt claimed the send and stopped before its receipt was
  // recorded: the operation is left running under a dead generation.
  let seeded = false;
  const plan = new Script("model-a", {
    "in-1": [
      { calls: [sendCall] },
      { text: "I cannot tell whether it went out." },
    ],
  });
  let failRound1 = true;
  const provider: ModelProvider = {
    name: "model-a",
    async *stream(req) {
      if (!seeded) {
        seeded = true;
        state.ops.set(`${PERSONA}|messaging.send|in-1:tool:0`, {
          persona_id: PERSONA,
          operation_id: "op-left-running",
          turn_id: "t-dead",
          tool: "messaging.send",
          idempotency_key: "in-1:tool:0",
          request: sendCall.request,
          status: "running",
          response: null,
          claimed_generation: -1,
          created_at: new Date().toISOString(),
          completed_at: null,
        });
      }
      if (req.round === 1 && failRound1) {
        // The attempt that finalized the receipt then fails transiently;
        // the next attempt replays the same position.
        failRound1 = false;
        plan.requests.push(structuredClone({ ...req, signal: undefined }));
        throw new ModelError("upstream 503", { retryable: true });
      }
      yield* plan.stream(req);
    },
  };
  await serve(state, provider);
  requeueNow(state, "in-1");
  await serve(state, provider);
  assert.equal(sends.length, 0, "the ambiguous send was never re-run");
  const op = [...state.ops.values()].find(
    (o) => o.operation_id === "op-left-running",
  );
  assert.equal(op?.status, "failed");
  assert.deepEqual(op?.response, {
    error:
      "uncompleted operation: a prior attempt left it running and it was not retried; whether its effect took place is unknown",
  });
  // Both attempts were fed the stored receipt, byte for byte.
  const round1 = plan.requests.filter((r) => r.round === 1);
  assert.equal(round1.length, 2);
  const fed = round1.map((r) => r.messages.at(-1)!);
  assert.deepEqual(fed[0], fed[1]);
  assert.equal(fed[0]!.content, JSON.stringify(op?.response));
  // The journal holds the pair once, with that receipt.
  const results = (await state.events(PERSONA, 0)).filter(
    (e) => e.kind === "tool_result",
  );
  assert.equal(results.length, 1);
  assert.equal(results[0]!.payload.error, op?.response?.error);

  state.addInput(PERSONA, "in-2", "Did it go out?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const result = journalView(b.requests[0]!).find((m) => m.role === "tool");
  assert.deepEqual(result, fed[0]);
});

const ANNOUNCE = {
  tool: "message.send",
  route: "elevated" as const,
  request: { text: "全員へ: 会議は15:30からです" },
};

/**
 * in-1 sends a Messaging message and then pauses before finishing: an
 * elevated call of the same round awaits the human, the next round's model
 * call waits for budget, or it fails transiently. Returns the store and a
 * provider that finishes in-1 once it resumes.
 */
async function sentThenPaused(pause: "approval" | "budget" | "retry") {
  const { state, sends } = messagingState();
  state.addInput(PERSONA, "in-1", "Tell the group the meeting time.");
  const script = new Script("model-a", {
    "in-1":
      pause === "approval"
        ? [
            {
              text: "Posting, then asking to announce.",
              calls: [sendCall, ANNOUNCE],
            },
            { text: "Both done." },
          ]
        : [{ text: "Posting.", calls: [sendCall] }, { text: "Posted." }],
  });
  let pausedOnce = false;
  const provider: ModelProvider = {
    name: "model-a",
    async *stream(req) {
      if (
        pause !== "approval" &&
        req.inputId === "in-1" &&
        req.round === 1 &&
        !pausedOnce
      ) {
        pausedOnce = true;
        if (pause === "budget") {
          throw new BudgetWaitError({
            funding: { kind: "operator", id: "env" },
            needed_minor: 10,
            limit_minor: 1,
            spent_minor: 0,
            held_minor: 0,
            remaining_minor: 1,
            currency: "USD",
            pricing_revision: "fixture-rates-v1",
            bounded: false,
            estimate: { input_tokens: 1_000_000 },
          });
        }
        throw new ModelError("upstream 503", { retryable: true });
      }
      yield* script.stream(req);
    },
  };
  if (pause === "budget") {
    state.setUsageBudget("operator", "env", {
      limit_minor: 1,
      currency: "USD",
      rate_input_per_mtok: 1_000_000,
      rate_output_per_mtok: 2_000_000,
    });
  }
  await serve(state, provider);
  return { state, sends, provider };
}

/** in-1's journaled records, keyed the way the store keeps them once. */
async function lineage(state: FakeState, inputId: string) {
  const turns = new Set(
    [...state.turns.values()]
      .filter((t) => t.input_id === inputId)
      .map((t) => t.turn_id),
  );
  return (await state.events(PERSONA, 0)).filter(
    (e) =>
      turns.has(e.turn_id) ||
      (e.kind === "input_received" && e.payload.input_id === inputId),
  );
}

for (const pause of ["approval", "budget", "retry"] as const) {
  test(`a send made before a ${pause} pause is visible to other conversations meanwhile, and resuming neither repeats nor re-records it`, async () => {
    const { state, sends, provider } = await sentThenPaused(pause);
    assert.equal(sends.length, 1);
    const in1 = state.inputs.find((i) => i.input_id === "in-1")!;
    assert.notEqual(in1.status, "done", "the request is not over");

    // Another conversation is served while in-1 waits.
    state.addInput(PERSONA, "in-2", "What have you done so far?");
    const b = new Script("model-b", {});
    await serve(state, b);
    const waiting = journalView(b.requests[0]!);
    const sent = waiting.findIndex((m) =>
      m.toolCalls?.some((c) => c.name === "messaging.send"),
    );
    assert.ok(sent >= 0, "the send is in the other conversation's context");
    assert.deepEqual(
      waiting[sent]!.toolCalls?.[0]?.arguments,
      sendCall.request,
    );
    assert.deepEqual(waiting[sent + 1], {
      role: "tool",
      toolCallId: "call_in-1_0_0",
      name: "messaging.send",
      content: JSON.stringify(RECEIPT),
    });
    // Nothing that has not happened is shown as done: the announcement is
    // pending, and no later round was decided.
    assert.ok(
      !waiting.some((m) => m.toolCalls?.some((c) => c.name === "message.send")),
    );
    assert.ok(!waiting.some((m) => /^(Posted|Both done)\.$/.test(m.content)));
    const status = waiting[sent + 2]!;
    assert.equal(status.role, "user");
    assert.match(
      status.content,
      pause === "approval"
        ? /^\[Approval requested: your elevated call message\.send /
        : pause === "budget"
          ? /^\[This request paused here: its next model call is waiting for usage budget\./
          : /^\[This request paused here after a temporary model error\./,
    );

    // The waiting ends; in-1 resumes on a restarted host.
    if (pause === "approval") {
      const [appr] = (await state.listApprovals(PERSONA)).filter(
        (x) => x.status === "pending",
      );
      assert.ok(appr);
      await state.resolveApproval(PERSONA, appr.approval_id, {
        decision: "approve_once",
        decision_id: "d-1",
        decided_by_kind: "human",
        decided_by_id: HUMAN,
      });
    } else if (pause === "budget") {
      state.clearUsageBudget("operator", "env");
    } else {
      requeueNow(state, "in-1");
    }
    await serve(state, provider);
    assert.equal(
      state.inputs.find((i) => i.input_id === "in-1")!.status,
      "done",
    );
    assert.equal(sends.length, 1, "the send never ran again");

    // Every experience of in-1 is journaled exactly once.
    const records = await lineage(state, "in-1");
    const count = (kind: string, key: string, value: unknown) =>
      records.filter((e) => e.kind === kind && e.payload[key] === value).length;
    assert.equal(count("input_received", "input_id", "in-1"), 1);
    assert.equal(count("assistant_message", "round", 0), 1);
    assert.equal(count("assistant_message", "round", 1), 1);
    assert.equal(count("tool_call", "call_index", 0), 1);
    assert.equal(count("tool_result", "call_index", 0), 1);
    if (pause === "approval") {
      assert.equal(count("tool_call", "call_index", 1), 1);
      assert.equal(count("tool_result", "call_index", 1), 1);
      assert.equal(
        (await state.outbox(PERSONA, 0)).filter(
          (o) => o.kind === "secretary_message",
        ).length,
        1,
        "the approved announcement ran once",
      );
    }

    // A later conversation sees the send once, in the order it happened.
    state.addInput(PERSONA, "in-3", "Thanks.");
    const c = new Script("model-c", {});
    await serve(state, c);
    const after = journalView(c.requests[0]!);
    assert.equal(
      after.filter((m) => m.toolCalls?.some((x) => x.name === "messaging.send"))
        .length,
      1,
    );
    assert.equal(sends.length, 1);
  });
}

/**
 * A state client whose commits of completed turns fail the way `fail`
 * says while `down()` holds: a 503 outage stops the host with the turn left
 * running; a plain Error is an internal failure the secretary strikes.
 */
function commitsFail(
  state: FakeState,
  fail: () => Error,
  down: () => boolean,
): StateClient {
  return new Proxy(state, {
    get(target, key) {
      if (key === "commitTurn") {
        return async (...args: Parameters<StateClient["commitTurn"]>) => {
          if (args[3].outcome === "complete" && down()) throw fail();
          return target.commitTurn(...args);
        };
      }
      const v = Reflect.get(target, key, target);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
}

/** in-1 sends, then its host stops before the turn commits. */
async function sentThenStopped(extra?: (state: FakeState) => Promise<void>) {
  const { state, sends } = messagingState();
  await extra?.(state);
  state.addInput(PERSONA, "in-1", "Tell the group the meeting time.");
  const script = new Script("model-a", {
    "in-0": [
      { text: "Asking to announce.", calls: [ANNOUNCE] },
      { text: "Announced." },
    ],
    "in-1": [{ text: "Posting.", calls: [sendCall] }, { text: "Posted." }],
  });
  let outage = true;
  const s = new Secretary(
    cfg(
      commitsFail(
        state,
        () => {
          outage = false;
          return new StateError(503, "state unavailable");
        },
        () => outage,
      ),
      script,
    ),
  );
  await s.start();
  for (let i = 0; i < 5 && outage; i++) {
    await s.step({ startMemory: false }).catch(() => {});
  }
  await s.stop();
  assert.equal(outage, false, "the host stopped at in-1's commit");
  assert.equal(sends.length, 1);
  return { state, sends, script };
}

/** The in-1 send as a later provider input carries it: the round's text
 * with the call, the receipt as its tool result, then the status line. */
function assertSendShown(
  messages: ChatMessage[],
  status: RegExp | null,
): number {
  const at = messages.findIndex((m) =>
    m.toolCalls?.some((c) => c.name === "messaging.send"),
  );
  assert.ok(at >= 0, "the send is in the provider input");
  assert.deepEqual(messages[at], {
    role: "assistant",
    content: "Posting.",
    toolCalls: [
      {
        id: "call_in-1_0_0",
        name: "messaging.send",
        route: "normal",
        arguments: sendCall.request,
      },
    ],
  });
  assert.deepEqual(messages[at + 1], {
    role: "tool",
    toolCallId: "call_in-1_0_0",
    name: "messaging.send",
    content: JSON.stringify(RECEIPT),
  });
  if (status) {
    assert.equal(messages[at + 2]!.role, "user");
    assert.match(messages[at + 2]!.content, status);
  }
  assert.equal(
    messages.filter((m) =>
      m.toolCalls?.some((c) => c.name === "messaging.send"),
    ).length,
    1,
    "shown once",
  );
  return at;
}

const INTERRUPTED =
  /^\[This request stopped here when its run was interrupted\. What is recorded above for it happened as shown; it resumes later, and the calls above are not run again\.\]$/;

test("a host that stops after a send: the send is journaled at its claim, and the resumed attempt adds it no second time", async () => {
  const { state, sends, script } = await sentThenStopped();
  // Journaled with the effect, before any commit.
  assert.deepEqual(
    (await lineage(state, "in-1")).map((e) => e.kind),
    ["input_received", "assistant_message", "tool_call", "tool_result"],
  );

  state.addInput(PERSONA, "in-2", "What did you post?");
  await serve(state, script);
  assert.equal(sends.length, 1, "the send never ran again");
  assert.deepEqual(
    (await lineage(state, "in-1")).map(
      (e) =>
        `${e.kind}:${e.payload.reason ?? e.payload.call_index ?? e.payload.round ?? ""}`,
    ),
    [
      "input_received:",
      "assistant_message:0",
      "tool_call:0",
      "tool_result:0",
      "turn_paused:interrupted",
      "assistant_message:1",
    ],
  );
  const in2 = script.requests.find((r) => r.inputId === "in-2")!;
  assertSendShown(journalView(in2), INTERRUPTED);
});

test("an older input served first after a stop sees the interrupted send; the interrupted one then finishes without sending again", async () => {
  // in-0 (older) parks on an elevated announcement; in-1 then sends and
  // its host stops before committing; the human approves in-0 meanwhile.
  const { state, sends, script } = await sentThenStopped(async (state) => {
    state.addInput(PERSONA, "in-0", "Announce the meeting to everyone.");
  });
  const [appr] = (await state.listApprovals(PERSONA)).filter(
    (x) => x.status === "pending",
  );
  assert.ok(appr, "in-0 parked before in-1 ran");
  await state.resolveApproval(PERSONA, appr.approval_id, {
    decision: "approve_once",
    decision_id: "d-1",
    decided_by_kind: "human",
    decided_by_id: HUMAN,
  });

  const before = script.requests.length;
  await serve(state, script);
  // in-0 resumes first and consults the model for its next round; in-1
  // then finishes from its recorded plan (its final round was decided
  // before the stop), so it needs no consultation.
  const served = script.requests.slice(before).map((r) => r.inputId);
  assert.deepEqual(served, ["in-0"], "the older input resumed first");

  // The actual provider input of in-0's resumed consultation: in-1's send
  // and receipt, marked as stopped, then in-0 itself and its own replayed
  // round — its approved announcement with the receipt it just got.
  const resumed = script.requests[before]!;
  const at = assertSendShown(resumed.messages, INTERRUPTED);
  assert.equal(resumed.messages[at - 1]!.role, "user");
  assert.match(
    resumed.messages[at - 1]!.content,
    /Tell the group the meeting time\.$/,
  );
  assert.match(
    resumed.messages[at + 3]!.content,
    /Announce the meeting to everyone\.$/,
  );
  assert.deepEqual(
    resumed.messages
      .slice(at + 4)
      .map((m) => [m.role, m.toolCalls?.[0]?.name ?? m.name ?? ""]),
    [
      ["assistant", "message.send"],
      ["tool", "message.send"],
    ],
  );

  assert.equal(sends.length, 1, "the interrupted send never ran again");
  assert.equal(
    (await state.outbox(PERSONA, 0)).filter(
      (o) => o.kind === "secretary_message",
    ).length,
    1,
    "the approved announcement ran once",
  );
  const records = await lineage(state, "in-1");
  for (const kind of ["tool_call", "tool_result", "turn_paused"]) {
    assert.equal(records.filter((e) => e.kind === kind).length, 1, kind);
  }
  assert.equal(state.inputs.find((i) => i.input_id === "in-1")!.status, "done");

  // Everything after reads each thing once, in the order it happened.
  state.addInput(PERSONA, "in-2", "Thanks.");
  const c = new Script("model-c", {});
  await serve(state, c);
  const after = journalView(c.requests[0]!);
  // The human's decision on in-0 landed while in-1's host was down, before
  // recovery marked in-1 stopped: the journal keeps that order.
  const later = assertSendShown(after, null);
  assert.match(
    after[later + 2]!.content,
    /^\[Approval decided: approve_once by human /,
  );
  assert.match(after[later + 3]!.content, INTERRUPTED);
  assert.deepEqual(
    after.filter((m) => m.role === "assistant").map((m) => m.content),
    // in-0's deciding text stands with its approval request; the approved
    // call ran later, in its own turn, as a call without new text.
    ["Asking to announce.", "Posting.", "", "Announced.", "Posted."],
  );
});

test("a request abandoned at the attempt cap after an interrupted send leaves the send and the failure in later context", async () => {
  const { state, sends } = await sentThenStopped();
  // The restarted host is past in-1's attempt allowance.
  const capped = new Script("model-b", {});
  await serve(state, capped, { maxAttempts: 1, providerRetryBudgetMs: 0 });
  assert.equal(capped.requests.length, 0, "no model was consulted for in-1");
  assert.equal(sends.length, 1);
  assert.equal(state.inputs.find((i) => i.input_id === "in-1")!.status, "done");

  state.addInput(PERSONA, "in-2", "Did it go out?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const view = journalView(b.requests[0]!);
  const at = assertSendShown(view, INTERRUPTED);
  assert.equal(
    view[at + 3]!.content,
    "[turn failed — this request stopped here without finishing normally. What is recorded above for it happened as shown — for example, a message you sent stays sent; nothing further runs for it]",
  );
  assert.equal(view.length, at + 4);
  assert.equal(sends.length, 1);
});

test("a request that fails on a recurring internal error after sending leaves the send and the failure in later context", async () => {
  const { state, sends } = messagingState();
  state.addInput(PERSONA, "in-1", "Tell the group the meeting time.");
  const script = new Script("model-a", {
    "in-1": [{ text: "Posting.", calls: [sendCall] }, { text: "Posted." }],
  });
  // Every completed commit of in-1 hits an internal fault: three strikes on
  // the same turn end it as a recorded failure.
  const s = new Secretary(
    cfg(
      commitsFail(
        state,
        () => new Error("invariant violated"),
        () => true,
      ),
      script,
    ),
  );
  await s.start();
  for (let i = 0; i < 3; i++) {
    await s.step({ startMemory: false }).catch(() => {});
  }
  await s.stop();
  assert.equal(state.inputs.find((i) => i.input_id === "in-1")!.status, "done");
  assert.equal(sends.length, 1, "each strike replayed the stored receipt");

  state.addInput(PERSONA, "in-2", "Did it go out?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const view = journalView(b.requests[0]!);
  const at = assertSendShown(
    view,
    /^\[turn failed — this request stopped here without finishing normally\. /,
  );
  assert.equal(view.length, at + 3);
  assert.deepEqual(
    (await lineage(state, "in-1")).map((e) => e.kind),
    [
      "input_received",
      "assistant_message",
      "tool_call",
      "tool_result",
      "turn_failed",
    ],
  );
});

test("the memory-preparation branch renders the same experience as the next turn", async () => {
  const { state, sends } = messagingState();
  // Long enough that the first exchange seals as a preparation target.
  const pad = "x".repeat(44 * 1024);
  state.addInput(
    PERSONA,
    "in-1",
    `Please tell the group the meeting time. ${pad}`,
  );
  state.addInput(PERSONA, "in-2", "Thanks.");
  const a = new Script("model-a", {
    "in-1": [{ text: "Posting it now.", calls: [sendCall] }, { text: "Sent." }],
  });
  await serve(state, a);

  // The next turn's view of the same journal.
  state.addInput(PERSONA, "in-3", "What did you post?");
  const b = new Script("model-b", {});
  await serve(state, b);
  const turn = b.requests[0]!;

  // A branch claimed now renders the parent context over the same journal.
  const lease = await state.acquireWriter(PERSONA, "memory-test", 30_000);
  await state.memoryMaintain(PERSONA, lease.generation);
  const branch = new Script("model-b", {});
  const result = await runMemoryPreparation({
    personaId: PERSONA,
    generation: lease.generation,
    state,
    provider: branch,
    contextLimit: 200,
    system: turn.messages[0]!.content,
    tools: turn.tools as ToolSpec[],
  });
  assert.equal(result, "worked");
  const req = branch.requests[0]!;
  const branchView = req.messages.slice(1, -1);
  // Same prefix as the turn, then in-3's own records that were journaled
  // since: the current input and its reply.
  assert.deepEqual(
    branchView.slice(0, turn.messages.length - 2),
    journalView(turn),
  );
  assert.deepEqual(branchView.slice(turn.messages.length - 2), [
    turn.messages.at(-1),
    { role: "assistant", content: "reply to in-3" },
  ]);
  const sent = branchView.find((m) =>
    m.toolCalls?.some((c) => c.arguments.content === SENT),
  );
  assert.ok(sent, "the branch context carries the sent body as the call");
  // The target carries the stored records verbatim, the call included.
  const target = req.messages.at(-1)!.content;
  assert.ok(target.startsWith(COMPACT_L1_PROMPT));
  const events = (
    JSON.parse(target.slice(target.indexOf("compact_target\n") + 15)) as {
      events: { kind: string; payload: Record<string, unknown> }[];
    }
  ).events;
  const call = events.find((e) => e.kind === "tool_call");
  assert.deepEqual(call?.payload.request, sendCall.request);
  assert.equal(call?.payload.round, 0);
  assert.equal(sends.length, 1, "preparation ran no tool");
});

function ev(
  seq: number,
  turn: string,
  kind: string,
  payload: Record<string, unknown>,
): Event {
  return {
    persona_id: PERSONA,
    seq,
    turn_id: turn,
    kind,
    payload,
    created_at: "2026-09-29T00:00:00.000Z",
  };
}

test("one round's calls share its deciding message; the next round is its own", () => {
  const view = renderJournalContext([
    ev(1, "t1", "input_received", { text: "do both", actor_kind: "human" }),
    ev(2, "t1", "assistant_message", { text: "Two things.", round: 0 }),
    ev(3, "t1", "tool_call", {
      tool: "journal.note",
      call_id: "c1",
      request: { text: "a" },
      route: "normal",
      round: 0,
    }),
    ev(4, "t1", "tool_result", {
      tool: "journal.note",
      call_id: "c1",
      response: { seq: 9 },
    }),
    ev(5, "t1", "tool_call", {
      tool: "messaging.send",
      call_id: "c2",
      request: { content: SENT },
      route: "normal",
      round: 0,
    }),
    ev(6, "t1", "tool_result", {
      tool: "messaging.send",
      call_id: "c2",
      response: RECEIPT,
    }),
    // Round 1 decided after seeing both results.
    ev(7, "t1", "tool_call", {
      tool: "messaging.edit_message",
      call_id: "c1",
      request: { content: EDITED },
      route: "normal",
      round: 1,
    }),
    ev(8, "t1", "tool_result", {
      tool: "messaging.edit_message",
      call_id: "c1",
      error: "not your message",
    }),
    ev(9, "t1", "assistant_message", { text: "Done.", round: 2 }),
  ]);
  assert.deepEqual(
    view.map((m) => [
      m.role,
      m.toolCalls?.map((c) => c.id) ?? m.toolCallId ?? m.content,
    ]),
    [
      ["user", "[Recorded 2026-09-29 00:00:00 UTC]\n[human] do both"],
      ["assistant", ["c1", "c2"]],
      ["tool", "c1"],
      ["tool", "c2"],
      // The provider reused an id across rounds. The record keeps it; each
      // wire adapter makes ids unique for its request (see the wire tests).
      ["assistant", ["c1"]],
      ["tool", "c1"],
      ["assistant", "Done."],
    ],
  );
  assert.equal(view[1]!.content, "Two things.");
  assert.equal(view[5]!.content, JSON.stringify({ error: "not your message" }));
});

/**
 * A journal the wire must carry: a result whose call fell outside the raw
 * window, a mock-style id no wire accepts, a pair, a reused id, and a
 * pending approval note.
 */
function wireJournal(): ChatMessage[] {
  return [
    { role: "system", content: "system" },
    ...renderJournalContext([
      ev(10, "t0", "tool_result", {
        tool: "journal.note",
        call_id: "old",
        response: { seq: 3 },
      }),
      ev(11, "t1", "input_received", { text: "post", actor_kind: "human" }),
      ev(12, "t1", "assistant_message", { text: "Posting.", round: 0 }),
      ev(13, "t1", "tool_call", {
        tool: "messaging.send",
        call_id: "call-messaging.send-0",
        request: { place_id: PLACE, content: SENT },
        route: "normal",
        round: 0,
      }),
      ev(14, "t1", "tool_result", {
        tool: "messaging.send",
        call_id: "call-messaging.send-0",
        response: RECEIPT,
      }),
      ev(15, "t1", "assistant_message", { text: "Sent.", round: 1 }),
      ev(16, "t2", "input_received", { text: "fix", actor_kind: "human" }),
      ev(17, "t2", "tool_call", {
        tool: "messaging.edit_message",
        call_id: "call-messaging.send-0",
        request: editCall.request,
        route: "normal",
        round: 0,
      }),
      ev(18, "t2", "tool_result", {
        tool: "messaging.edit_message",
        call_id: "call-messaging.send-0",
        response: { message_id: RECEIPT.message_id },
      }),
      ev(19, "t2", "approval_requested", {
        tool: "message.send",
        call_id: "c9",
        route: "elevated",
        round: 1,
        approval_id: "a-1",
        request: { text: "all" },
      }),
    ]),
    { role: "user", content: "[human] what did you post?" },
  ];
}

const WIRE_TOOLS: ToolSpec[] = [
  {
    name: "messaging.send",
    description: "send",
    parameters: { type: "object", properties: {} },
  },
];

function wireRequest(): ModelRequest {
  return {
    personaId: PERSONA,
    turnId: "t3",
    round: 0,
    messages: wireJournal(),
    tools: WIRE_TOOLS,
  };
}

/** Capture the request body a provider puts on the wire. */
async function wireBody(
  make: (f: typeof fetch) => ModelProvider,
  request: ModelRequest = wireRequest(),
): Promise<Record<string, unknown>> {
  let body = "";
  const f = (async (_u: unknown, init?: RequestInit) => {
    // The first request is the one built from the messages; a refused
    // request with continuation is resent once without it.
    body ||= String(init?.body);
    return new Response('{"error":{"message":"captured"}}', { status: 400 });
  }) as typeof fetch;
  await assert.rejects(async () => {
    for await (const _ of make(f).stream(request));
  });
  return JSON.parse(body) as Record<string, unknown>;
}

/** Anthropic's documented tool_use id pattern. Chat Completions and
 * Responses document no charset: ids there are non-empty and unique. */
const TOOL_USE_ID = /^[a-zA-Z0-9_-]+$/;

test("what an effect records inside its claim follows the round's results, never splitting a call from its result", () => {
  const view = renderJournalContext([
    ev(1, "t1", "input_received", { text: "note it", actor_kind: "human" }),
    ev(2, "t1", "assistant_message", { text: "Noting.", round: 0 }),
    ev(3, "t1", "tool_call", {
      tool: "journal.note",
      call_id: "n1",
      request: { text: "remember" },
      route: "normal",
      round: 0,
      call_index: 0,
    }),
    ev(4, "t1", "note", { text: "remember" }),
    ev(5, "t1", "tool_result", {
      tool: "journal.note",
      call_id: "n1",
      call_index: 0,
      response: { seq: 4, kind: "note" },
    }),
    ev(6, "t1", "assistant_message", { text: "Noted.", round: 1 }),
  ]);
  assert.deepEqual(
    view.map((m) => [m.role, m.content]),
    [
      ["user", view[0]!.content],
      ["assistant", "Noting."],
      ["tool", '{"seq":4,"kind":"note"}'],
      ["assistant", "[note] remember"],
      ["assistant", "Noted."],
    ],
  );
  assert.equal(view[1]!.toolCalls?.[0]?.id, "n1");
  assert.equal(view[2]!.toolCallId, "n1");
});

test("chat completions wire: every rendered call is answered, with unique ids", async () => {
  const body = await wireBody(
    (f) =>
      new OpenAIProvider(
        { baseUrl: "http://x.invalid", apiKey: "k", model: "m" },
        f,
      ),
  );
  const msgs = body.messages as {
    role: string;
    content: string;
    tool_calls?: {
      id: string;
      function: { name: string; arguments: string };
    }[];
    tool_call_id?: string;
  }[];
  const seen = new Set<string>();
  for (let i = 0; i < msgs.length; i++) {
    const m = msgs[i]!;
    assert.notEqual(m.role, "tool", "no result without its call");
    if (!m.tool_calls) continue;
    const ids = m.tool_calls.map((c) => c.id);
    const answers = msgs.slice(i + 1, i + 1 + ids.length);
    assert.deepEqual(
      answers.map((x) => [x.role, x.tool_call_id]),
      ids.map((id) => ["tool", id]),
    );
    for (const id of ids) {
      assert.notEqual(id, "");
      assert.ok(!seen.has(id), `unique id ${id}`);
      seen.add(id);
    }
    i += ids.length;
  }
  const send = msgs.find(
    (m) => m.tool_calls?.[0]?.function.name === "messaging_send",
  );
  assert.ok(send);
  assert.equal(send.content, "Posting.");
  assert.deepEqual(JSON.parse(send.tool_calls![0]!.function.arguments), {
    route: "normal",
    input: { place_id: PLACE, content: SENT },
  });
  // The edit's tool is no longer advertised; its wire name still maps.
  assert.ok(
    msgs.some(
      (m) => m.tool_calls?.[0]?.function.name === "messaging_edit_message",
    ),
  );
  assert.equal(seen.size, 2);
});

test("Anthropic wire: each tool_use is answered by a tool_result in the next user message", async () => {
  const body = await wireBody(
    (f) =>
      new AnthropicProvider(
        { baseUrl: "http://x.invalid", apiKey: "k", model: "m" },
        f,
      ),
  );
  const msgs = body.messages as {
    role: string;
    content: {
      type: string;
      id?: string;
      tool_use_id?: string;
      input?: unknown;
    }[];
  }[];
  // Strict alternation.
  msgs.forEach((m, i) => {
    if (i > 0) assert.notEqual(m.role, msgs[i - 1]!.role);
  });
  const all = new Set<string>();
  msgs.forEach((m, i) => {
    const uses = m.content.filter((b) => b.type === "tool_use");
    const results = m.content.filter((b) => b.type === "tool_result");
    if (i === 0 || msgs[i - 1]!.content.every((b) => b.type !== "tool_use")) {
      assert.equal(
        results.length,
        0,
        "no tool_result without a preceding tool_use",
      );
    }
    if (uses.length === 0) return;
    const next = msgs[i + 1]!;
    assert.deepEqual(
      next.content.slice(0, uses.length).map((b) => [b.type, b.tool_use_id]),
      uses.map((u) => ["tool_result", u.id]),
    );
    for (const u of uses) {
      assert.match(u.id!, TOOL_USE_ID);
      assert.ok(!all.has(u.id!));
      all.add(u.id!);
    }
  });
  assert.equal(all.size, 2);
  const send = msgs
    .flatMap((m) => m.content)
    .find(
      (b) => b.type === "tool_use" && JSON.stringify(b.input).includes(SENT),
    );
  assert.ok(send, "the sent body is carried by its tool_use");
});

for (const dialect of ["standard", "chatgpt"] as const) {
  test(`Responses wire (${dialect}): function_call items are each followed by their output`, async () => {
    const body = await wireBody(
      (f) =>
        new OpenAIResponsesProvider(
          {
            baseUrl: "http://x.invalid",
            apiKey: "k",
            model: "gpt-5",
            ...(dialect === "chatgpt"
              ? {
                  chatgpt: {
                    accountId: "acct",
                    send: (b: string, signal: AbortSignal) =>
                      f("http://x.invalid/responses", {
                        method: "POST",
                        body: b,
                        signal,
                      }),
                  },
                }
              : {}),
          },
          f,
        ),
    );
    const input = body.input as {
      type: string;
      call_id?: string;
      arguments?: string;
    }[];
    const calls = input.filter((x) => x.type === "function_call");
    const outputs = input.filter((x) => x.type === "function_call_output");
    assert.equal(calls.length, 2);
    assert.deepEqual(
      outputs.map((o) => o.call_id),
      calls.map((c) => c.call_id),
    );
    for (const c of calls) {
      assert.notEqual(c.call_id, "");
      const at = input.indexOf(c);
      assert.equal(input[at + 1]?.type, "function_call_output");
      assert.equal(input[at + 1]?.call_id, c.call_id);
    }
    assert.equal(new Set(calls.map((c) => c.call_id)).size, 2);
    assert.ok(calls.some((c) => c.arguments?.includes(SENT)));
  });
}

/**
 * A provider that mints the same call id every round (sequential-id
 * providers do): history turns and the current turn all carry "repeatable".
 */
async function repeatedIdRequest(): Promise<ModelRequest> {
  const { state } = messagingState();
  const requests: ModelRequest[] = [];
  const provider: ModelProvider = {
    name: "repeat",
    async *stream(req) {
      requests.push(structuredClone({ ...req, signal: undefined }));
      if (req.round === 0) {
        yield {
          type: "tool_call",
          call: {
            id: "repeatable",
            name: "messaging.send",
            route: "normal",
            arguments: { place_id: PLACE, content: `${req.inputId}` },
          },
        };
      } else {
        yield { type: "text", delta: "ok" };
      }
      yield { type: "done", usage: {} };
    },
  };
  for (const id of ["in-1", "in-2", "in-3"]) {
    state.addInput(PERSONA, id, `send ${id}`);
  }
  await serve(state, provider);
  // in-3's second consultation: two history pairs and its own, all
  // "repeatable".
  const req = requests.find((r) => r.inputId === "in-3" && r.round === 1)!;
  const ids = req.messages.flatMap((m) => m.toolCalls?.map((c) => c.id) ?? []);
  assert.deepEqual(ids, ["repeatable", "repeatable", "repeatable"]);
  return req;
}

/** Each call id on the wire, in order, with the content of its answer. */
function pairsOf(
  calls: { id: string; content: string }[],
  answers: { id: string; content: string }[],
) {
  assert.equal(new Set(calls.map((c) => c.id)).size, calls.length);
  assert.deepEqual(
    answers.map((a) => a.id),
    calls.map((c) => c.id),
  );
  return calls.map((c, i) => [
    c.id,
    JSON.parse(c.content).input.content,
    answers[i]!.content,
  ]);
}

test("repeated call ids across history and the current turn are unique on every wire, still paired", async () => {
  const req = await repeatedIdRequest();
  const expectSent = ["in-1", "in-2", "in-3"];

  const chat = (
    await wireBody(
      (f) =>
        new OpenAIProvider(
          { baseUrl: "http://x.invalid", apiKey: "k", model: "m" },
          f,
        ),
      req,
    )
  ).messages as {
    role: string;
    content: string;
    tool_call_id?: string;
    tool_calls?: { id: string; function: { arguments: string } }[];
  }[];
  const chatPairs = pairsOf(
    chat.flatMap(
      (m) =>
        m.tool_calls?.map((c) => ({
          id: c.id,
          content: c.function.arguments,
        })) ?? [],
    ),
    chat
      .filter((m) => m.role === "tool")
      .map((m) => ({ id: m.tool_call_id!, content: m.content })),
  );
  assert.deepEqual(
    chatPairs.map((p) => p[1]),
    expectSent,
  );
  // The first occurrence keeps the provider's id; later ones are renamed.
  assert.deepEqual(
    chatPairs.map((p) => p[0]),
    ["repeatable", "sumi_call_0", "sumi_call_1"],
  );

  const anthropic = (
    await wireBody(
      (f) =>
        new AnthropicProvider(
          { baseUrl: "http://x.invalid", apiKey: "k", model: "m" },
          f,
        ),
      req,
    )
  ).messages as {
    content: {
      type: string;
      id?: string;
      tool_use_id?: string;
      input?: unknown;
      content?: string;
    }[];
  }[];
  const blocks = anthropic.flatMap((m) => m.content);
  const antPairs = pairsOf(
    blocks
      .filter((b) => b.type === "tool_use")
      .map((b) => ({ id: b.id!, content: JSON.stringify(b.input) })),
    blocks
      .filter((b) => b.type === "tool_result")
      .map((b) => ({ id: b.tool_use_id!, content: b.content! })),
  );
  assert.deepEqual(
    antPairs.map((p) => p[1]),
    expectSent,
  );
  for (const [id] of antPairs) assert.match(id as string, TOOL_USE_ID);

  const responses = (
    await wireBody(
      (f) =>
        new OpenAIResponsesProvider(
          { baseUrl: "http://x.invalid", apiKey: "k", model: "gpt-5" },
          f,
        ),
      req,
    )
  ).input as {
    type: string;
    call_id?: string;
    arguments?: string;
    output?: string;
  }[];
  const rPairs = pairsOf(
    responses
      .filter((x) => x.type === "function_call")
      .map((x) => ({ id: x.call_id!, content: x.arguments! })),
    responses
      .filter((x) => x.type === "function_call_output")
      .map((x) => ({ id: x.call_id!, content: x.output! })),
  );
  assert.deepEqual(
    rPairs.map((p) => p[1]),
    expectSent,
  );
  // Results stay with their own calls: each answer is its own receipt.
  assert.deepEqual(
    rPairs.map((p) => p[2]),
    chatPairs.map((p) => p[2]),
  );
});

/** Every request body a provider sends for `request`, in order; each is
 * refused (400), so a continuation-bearing request is resent once without
 * its continuation. */
async function wireBodies(
  make: (f: typeof fetch) => ModelProvider,
  request: ModelRequest,
): Promise<Record<string, unknown>[]> {
  const bodies: Record<string, unknown>[] = [];
  const f = (async (_u: unknown, init?: RequestInit) => {
    bodies.push(JSON.parse(String(init?.body)));
    return new Response('{"error":{"message":"captured"}}', { status: 400 });
  }) as typeof fetch;
  await assert.rejects(async () => {
    for await (const _ of make(f).stream(request));
  });
  return bodies;
}

const chatgptProvider = (f: typeof fetch) =>
  new OpenAIResponsesProvider(
    {
      baseUrl: "http://x.invalid",
      apiKey: "k",
      model: "gpt-5",
      chatgpt: {
        accountId: "acct",
        send: (b: string, signal: AbortSignal) =>
          f("http://x.invalid/responses", { method: "POST", body: b, signal }),
      },
    },
    f,
  );

test("ChatGPT continuation ids stay verbatim, and earlier ids never change for them", async () => {
  const req = await repeatedIdRequest();
  const scope = await continuationScope("acct", "gpt-5");
  // The current round carries its continuation: opaque reasoning plus a
  // reference to the call by call_id and item id. The provider minted
  // "repeatable" after receiving the history that already used it.
  const current = req.messages.findLast((m) => m.role === "assistant")!;
  current.continuation = {
    scope,
    output: [
      { type: "reasoning", id: "rs_1", summary: [], encrypted_content: "ENC" },
      { type: "function_call", id: "fc_1", call_id: "repeatable" },
    ],
  };
  const [body, resend] = await wireBodies(chatgptProvider, req);
  type Item = {
    type: string;
    id?: string;
    call_id?: string;
    encrypted_content?: string;
    arguments?: string;
  };
  const input = body!.input as Item[];
  const calls = input.filter((x) => x.type === "function_call");
  // History keeps the ids it serialized with before this round existed;
  // the continuation's call keeps the id its opaque items reference.
  assert.deepEqual(
    calls.map((c) => c.call_id),
    ["repeatable", "sumi_call_0", "repeatable"],
  );
  const cont = calls.find((c) => c.id === "fc_1");
  assert.equal(cont, calls[2]);
  assert.equal(JSON.parse(cont!.arguments!).input.content, "in-3");
  assert.ok(
    input.some((x) => x.type === "reasoning" && x.encrypted_content === "ENC"),
  );
  // Each call is answered right after it, by its own id.
  for (const c of calls) {
    const out = input
      .slice(input.indexOf(c) + 1)
      .find((x) => x.type === "function_call_output");
    assert.equal(out?.call_id, c.call_id);
    assert.equal(input[input.indexOf(c) + 1], out);
  }
  // Refused, it is resent explicitly without the continuation; nothing
  // then references the round's ids, and every id is unique.
  const again = (resend!.input as Item[]).filter(
    (x) => x.type === "function_call",
  );
  assert.ok(!(resend!.input as Item[]).some((x) => x.type === "reasoning"));
  assert.deepEqual(
    again.map((c) => c.call_id),
    ["repeatable", "sumi_call_0", "sumi_call_1"],
  );
});

test("appending rounds never changes how the earlier request serialized, on every wire", async () => {
  const req = await repeatedIdRequest();
  const scope = await continuationScope("acct", "gpt-5");
  // A frozen prefix (a parent request) and the same prefix with a branch
  // round appended whose continuation-bearing call repeats a history id.
  const prefix: ModelRequest = structuredClone(req);
  const branch: ModelRequest = structuredClone(req);
  branch.messages.push(
    { role: "user", content: "branch instruction" },
    {
      role: "assistant",
      content: "",
      toolCalls: [
        {
          id: "sumi_call_0",
          name: "messaging.send",
          route: "normal",
          arguments: { place_id: PLACE, content: "branch" },
        },
      ],
      continuation: {
        scope,
        output: [
          {
            type: "reasoning",
            id: "rs_b",
            summary: [],
            encrypted_content: "B",
          },
          { type: "function_call", id: "fc_b", call_id: "sumi_call_0" },
        ],
      },
    },
    {
      role: "tool",
      toolCallId: "sumi_call_0",
      name: "messaging.send",
      content: "{}",
    },
  );
  const wires: [string, (f: typeof fetch) => ModelProvider, string][] = [
    [
      "chat",
      (f) =>
        new OpenAIProvider(
          { baseUrl: "http://x.invalid", apiKey: "k", model: "m" },
          f,
        ),
      "messages",
    ],
    [
      "anthropic",
      (f) =>
        new AnthropicProvider(
          { baseUrl: "http://x.invalid", apiKey: "k", model: "m" },
          f,
        ),
      "messages",
    ],
    ["chatgpt", chatgptProvider, "input"],
  ];
  for (const [wire, make, key] of wires) {
    const [before] = await wireBodies(make, prefix);
    const [after] = await wireBodies(make, branch);
    const a = before![key] as unknown[];
    const b = after![key] as unknown[];
    assert.ok(b.length > a.length, wire);
    // Anthropic merges consecutive same-role messages, so the prefix's
    // final user turn (its tool results) also takes the appended
    // instruction; everything before it is byte-identical.
    const stable = wire === "anthropic" ? a.length - 1 : a.length;
    assert.deepEqual(b.slice(0, stable), a.slice(0, stable), wire);
  }
});
