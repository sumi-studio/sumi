import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
} from "../src/provider.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { StateError } from "../src/state-client.ts";
import type { Approval, ApprovalDecision, ToolRoute } from "../src/types.ts";

/**
 * Tool authority through the whole secretary loop on the in-memory state
 * contract: a gated call waits for its human, resumes once after a restart,
 * and a denial is never run or quietly redone.
 */

const PERSONA = "01930e00-0000-7000-8000-0000000000a1";
const HUMAN = "01930e00-0000-7000-8000-0000000000b1";

type Round = {
  text: string;
  calls?: { tool: string; route: ToolRoute; request: Record<string, unknown> }[];
};

class Script implements ModelProvider {
  readonly name = "script";
  consulted: number[] = [];
  private readonly rounds: Round[];
  constructor(rounds: Round[]) {
    this.rounds = rounds;
  }
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.consulted.push(req.round);
    const r = this.rounds[req.round] ?? { text: "done" };
    yield { type: "text", delta: r.text };
    for (const [i, c] of (r.calls ?? []).entries()) {
      yield {
        type: "tool_call",
        call: {
          id: `c-${req.round}-${i}`,
          name: c.tool,
          route: c.route,
          arguments: c.request,
        },
      };
    }
    yield { type: "done", usage: {} };
  }
}

function cfg(
  state: FakeState,
  holder: string,
  over: Partial<SecretaryConfig> = {},
): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: holder,
    state,
    provider: new MockProvider(),
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 50,
    pollIntervalMs: 1,
    scheduleEveryMs: 60_000,
    idgen: () => crypto.randomUUID(),
    ...over,
  };
}

async function restart(
  prev: Secretary,
  state: FakeState,
  over: Partial<SecretaryConfig> = {},
): Promise<Secretary> {
  await prev.stop();
  const next = new Secretary(cfg(state, `h-${crypto.randomUUID()}`, over));
  await next.start();
  return next;
}

async function pending(state: FakeState): Promise<Approval[]> {
  return (await state.listApprovals(PERSONA)).filter(
    (a) => a.status === "pending",
  );
}

function decide(
  decision: ApprovalDecision["decision"],
  id: string,
  by = HUMAN,
): ApprovalDecision {
  return {
    decision,
    decision_id: id,
    decided_by_kind: "human",
    decided_by_id: by,
  };
}

async function kinds(state: FakeState): Promise<string[]> {
  return (await state.outbox(PERSONA, 0)).map((o) => o.kind);
}

test("message.send waits for the human, then sends exactly once after approval and restart", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", '!elevated message.send {"text":"会議を15時に移します"}');

  const s1 = new Secretary(cfg(state, "h-1"));
  await s1.start();
  assert.equal(await s1.step(), "turn");
  assert.deepEqual(await kinds(state), ["approval_requested"]);
  const [appr] = await pending(state);
  assert.ok(appr);
  assert.equal(appr.tool, "message.send");
  assert.equal(appr.required_by, "route");
  // Nothing retries behind the human's back while the request waits.
  assert.equal(await s1.step(), "idle");
  assert.equal(await s1.step(), "idle");
  assert.deepEqual(await kinds(state), ["approval_requested"]);

  await assert.rejects(
    state.resolveApproval(
      PERSONA,
      appr.approval_id,
      decide("approve_once", "d-x", "01930e00-0000-7000-8000-0000000000ff"),
    ),
    (e: unknown) => e instanceof StateError && e.status === 403,
  );
  await state.resolveApproval(
    PERSONA,
    appr.approval_id,
    decide("approve_once", "d-1"),
  );

  const s2 = await restart(s1, state);
  assert.equal(await s2.step(), "turn");
  assert.equal(await s2.step(), "idle");

  const outbox = await kinds(state);
  assert.equal(outbox.filter((k) => k === "secretary_message").length, 1);
  assert.equal(outbox.filter((k) => k === "turn_completed").length, 1);
  const evs = await state.events(PERSONA, 0);
  assert.equal(
    evs.filter((e) => e.kind === "input_received").length,
    1,
    "the resumed input is journaled once",
  );
  assert.deepEqual(
    evs.slice(0, 2).map((e) => e.kind),
    ["approval_requested", "approval_decided"],
  );
  const [after] = await state.listApprovals(PERSONA, appr.approval_id);
  assert.ok(after?.consumed_at, "the one-shot grant was consumed");
  await s2.stop();
});

test("a denied send is never delivered, and the model is told it was denied", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", '!elevated message.send {"text":"全員に送信"}');

  const s1 = new Secretary(cfg(state, "h-1"));
  await s1.start();
  await s1.step();
  const [appr] = await pending(state);
  assert.ok(appr);
  await state.resolveApproval(PERSONA, appr.approval_id, decide("deny_once", "d-1"));
  await assert.rejects(
    state.resolveApproval(PERSONA, appr.approval_id, decide("approve_once", "d-2")),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );

  const s2 = await restart(s1, state);
  assert.equal(await s2.step(), "turn");
  assert.equal(await s2.step(), "idle");
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.filter((o) => o.kind === "secretary_message").length, 0);
  const done = outbox.find((o) => o.kind === "turn_completed");
  const results = (
    done?.payload as { output: { tool_results: { result: unknown }[] } }
  ).output.tool_results;
  assert.deepEqual(results[0]?.result, { error: "denied" });
  const denied = (await state.events(PERSONA, 0)).find(
    (e) => e.kind === "tool_result",
  );
  assert.equal((denied?.payload as { denied?: boolean }).denied, true);
  await s2.stop();
});

test("after the human denies an elevated call, the model cannot redo it on its own authority", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", "note my bank PIN");
  const script = new Script([
    {
      text: "asking first",
      calls: [{ tool: "journal.note", route: "elevated", request: { text: "PIN 1234" } }],
    },
    {
      text: "writing anyway",
      calls: [{ tool: "journal.note", route: "normal", request: { text: "PIN 1234" } }],
    },
    { text: "I did not save it." },
  ]);

  const s1 = new Secretary(cfg(state, "h-1", { provider: script }));
  await s1.start();
  await s1.step();
  const [appr] = await pending(state);
  assert.equal(appr?.required_by, "route");
  await state.resolveApproval(PERSONA, appr!.approval_id, decide("deny_once", "d-1"));

  const s2 = await restart(s1, state, { provider: script });
  assert.equal(await s2.step(), "turn");
  const evs = await state.events(PERSONA, 0);
  assert.equal(evs.filter((e) => e.kind === "note").length, 0);
  const results = evs
    .filter((e) => e.kind === "tool_result")
    .map((e) => (e.payload as { error?: string }).error);
  assert.deepEqual(results, ["denied", "denied"]);
  assert.equal((await pending(state)).length, 0, "no second request was raised");
  await s2.stop();
});

test("several approvals for one request do not exhaust its attempt cap", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", "tell both teams");
  const script = new Script([
    { text: "a", calls: [{ tool: "message.send", route: "elevated", request: { text: "team A" } }] },
    { text: "b", calls: [{ tool: "message.send", route: "elevated", request: { text: "team B" } }] },
    { text: "both sent" },
  ]);
  // A tight cap with no provider budget: any spent attempt beyond the
  // first would abandon the request.
  const over = { provider: script, maxAttempts: 1, providerRetryBudgetMs: 0 };

  let s = new Secretary(cfg(state, "h-1", over));
  await s.start();
  for (let i = 0; i < 2; i++) {
    assert.equal(await s.step(), "turn");
    const [appr] = await pending(state);
    assert.ok(appr, `approval ${i} requested`);
    await state.resolveApproval(PERSONA, appr.approval_id, decide("approve_once", `d-${i}`));
    s = await restart(s, state, over);
  }
  assert.equal(await s.step(), "turn");
  const outbox = await kinds(state);
  assert.equal(outbox.filter((k) => k === "secretary_message").length, 2);
  assert.equal(outbox.filter((k) => k === "turn_completed").length, 1);
  await s.stop();
});

test("a normal-route message.send is blocked without ever asking the human", async () => {
  // ADR 0013 §2: Normal does not ask. The call is a durable structured
  // block the model sees as a tool error — no approval row, no prompt,
  // no delivery, and no silent re-route to elevated.
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", '!message.send {"text":"勝手に送信"}');

  const s = new Secretary(cfg(state, "h-1"));
  await s.start();
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");

  assert.equal((await pending(state)).length, 0, "no approval was requested");
  const outbox = await kinds(state);
  assert.equal(outbox.filter((k) => k === "approval_requested").length, 0);
  assert.equal(outbox.filter((k) => k === "secretary_message").length, 0);
  assert.equal(outbox.filter((k) => k === "turn_completed").length, 1);
  const results = state.eventLog.filter((e) => e.kind === "tool_result");
  assert.equal(results.length, 1);
  assert.equal(
    (results[0]?.payload as { error?: string }).error,
    "blocked",
  );
  await s.stop();
});

test("a gated call whose arguments can never run is not asked of the human", async () => {
  // F2: deterministic validation precedes the approval record — nothing
  // parks, no grant is stranded, the model sees the argument error.
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", "!elevated message.send {}");

  const s = new Secretary(cfg(state, "h-1"));
  await s.start();
  assert.equal(await s.step(), "turn");
  assert.equal((await pending(state)).length, 0, "invalid call parked");
  const results = state.eventLog.filter((e) => e.kind === "tool_result");
  assert.match(String((results[0]?.payload as { error?: string })?.error), /requires text/);
  await s.stop();
});

test("an empty decision_id is rejected before any mutation", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", '!elevated journal.note {"text":"x"}');
  const s = new Secretary(cfg(state, "h-1"));
  await s.start();
  await s.step();
  const [appr] = await pending(state);
  assert.ok(appr);
  await assert.rejects(
    state.resolveApproval(PERSONA, appr.approval_id, decide("approve_once", "")),
    (e: unknown) => e instanceof StateError && e.status === 400,
  );
  // Still pending and decidable.
  const got = await state.resolveApproval(
    PERSONA,
    appr.approval_id,
    decide("approve_once", "d-1"),
  );
  assert.equal(got.status, "approved");
  await s.stop();
});

test("a long human wait does not consume the provider retry window", async () => {
  // F4: approve after a wait far beyond the retry budget; the resume
  // executes the granted effect once, then a transient provider error on
  // the follow-up consult still retries — the wait is excluded from the
  // budget because it was recorded durably on the input.
  const state = new FakeState();
  state.addPersona(PERSONA, "secretary", HUMAN);
  state.addInput(PERSONA, "in-1", '!elevated message.send {"text":"遅い承認"}');

  // Round 0 parks; round 1 consults fail once transiently, then succeed.
  class Flaky implements ModelProvider {
    readonly name = "flaky";
    private streams = 0;
    async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
      this.streams++;
      if (req.round === 0) {
        yield {
          type: "tool_call",
          call: {
            id: "c-0-0",
            name: "message.send",
            route: "elevated",
            arguments: { text: "遅い承認" },
          },
        };
      } else if (this.streams === 2) {
        throw new ModelError("provider blip", { retryable: true });
      } else {
        yield { type: "text", delta: "done" };
      }
      yield { type: "done", usage: {} };
    }
  }
  const provider = new Flaky();
  const over = { provider, providerRetryBudgetMs: 60_000 };

  const s1 = new Secretary(cfg(state, "h-1", over));
  await s1.start();
  assert.equal(await s1.step(), "turn");
  const [appr] = await pending(state);
  assert.ok(appr);

  // Two hours pass while the human thinks — past the 60s budget.
  const input = state.inputs.find((i) => i.input_id === "in-1")!;
  input.waiting_since = new Date(Date.now() - 2 * 3600_000).toISOString();
  await state.resolveApproval(PERSONA, appr.approval_id, decide("approve_once", "d-1"));
  assert.ok(input.waited_ms >= 2 * 3600_000, "wait was recorded durably");

  const s2 = await restart(s1, state, over);
  // Resume: effect executes once, the round-1 blip is still retryable.
  assert.equal(await s2.step(), "turn");
  assert.equal(
    (await kinds(state)).filter((k) => k === "secretary_message").length,
    1,
    "granted effect ran exactly once",
  );
  assert.equal(
    (await kinds(state)).filter((k) => k === "turn_failed").length,
    0,
    "the wait exhausted the budget — retryable error became terminal",
  );
  await new Promise((r) => setTimeout(r, 300)); // not_before backoff
  assert.equal(await s2.step(), "turn");
  assert.equal(
    (await kinds(state)).filter((k) => k === "turn_completed").length,
    1,
  );
  assert.equal(
    (await kinds(state)).filter((k) => k === "secretary_message").length,
    1,
    "the send was not repeated by the retried attempt",
  );
  await s2.stop();
});

test("an approved grant on a tool with no effect settles failed, not parked forever", async () => {
  // f44 parity: a gated tool registered without an in-store effect would
  // otherwise consume the grant and leave the op awaiting_approval on every
  // re-claim. The grant is spent and the op settles as an honest failure.
  const { TOOL_AUTHORITY } = await import("../src/fake-state.ts");
  TOOL_AUTHORITY["phantom.gated"] = { requiresApproval: false, elevatedOnly: true };
  try {
    const state = new FakeState();
    state.addPersona(PERSONA, "secretary", HUMAN);
    state.addInput(PERSONA, "in-1", '!elevated phantom.gated {"text":"x"}');
    const s = new Secretary(cfg(state, "h-1"));
    await s.start();
    assert.equal(await s.step(), "turn");
    const [appr] = await pending(state);
    assert.ok(appr);
    await state.resolveApproval(PERSONA, appr.approval_id, decide("approve_once", "d-1"));
    const s2 = await restart(s, state);
    assert.equal(await s2.step(), "turn");
    const op = [...state.ops.values()].find((o) => o.tool === "phantom.gated")!;
    assert.equal(op.status, "failed");
    assert.match(String(op.response?.error), /no registered effect/);
    const got = (await state.listApprovals(PERSONA))[0]!;
    assert.equal(got.status, "approved");
    assert.notEqual(got.consumed_at, null, "the grant was spent once");
    await s2.stop();
  } finally {
    delete TOOL_AUTHORITY["phantom.gated"];
  }
});
