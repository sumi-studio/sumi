import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { NoSelectionProvider } from "../src/host/provider-env.ts";
import { eventMessage, formatGap, receiptLine } from "../src/memory.ts";
import type {
  ChatMessage,
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { StateError } from "../src/state-client.ts";
import type { CommitRequest } from "../src/types.ts";

const PERSONA = "01930e00-0000-7000-8000-0000000000c7";
const DAY = 86_400_000;

/** Records every send; answers with a fixed text. */
class RecordingProvider implements ModelProvider {
  readonly name: string;
  readonly requests: ModelRequest[] = [];
  constructor(name: string) {
    this.name = name;
  }
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(structuredClone(req));
    yield { type: "text", delta: `reply from ${this.name}` };
    yield { type: "done", usage: {} };
  }
}

function cfg(state: FakeState, provider: ModelProvider): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: "continuity-test",
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 200,
    pollIntervalMs: 1,
    scheduleEveryMs: 60_000,
    idgen: () => crypto.randomUUID(),
  };
}

/** Admit an input as if it had arrived at `at`. */
function arrive(state: FakeState, id: string, text: string, at: number) {
  state.addInput(PERSONA, id, text);
  const input = state.inputs.find((i) => i.input_id === id)!;
  input.created_at = new Date(at).toISOString();
}

/** Admission fixes the previous receipt before the next input arrives. */
function arriveAll(state: FakeState, items: [string, string, number][]) {
  for (const [id, text, at] of items) arrive(state, id, text, at);
}

const journalView = (req: ModelRequest): ChatMessage[] =>
  req.messages.slice(1, -1);

test("gaps render in at most two units, marked when rounded", () => {
  assert.equal(formatGap(90_000), "1 minute 30 seconds");
  assert.equal(formatGap(9_929), "approximately 10 seconds");
  assert.equal(formatGap(400), "less than a second");
  assert.equal(formatGap(59_600), "approximately 1 minute");
  assert.equal(formatGap(DAY), "1 day");
  assert.equal(formatGap(10 * DAY + 3 * 3_600_000), "10 days 3 hours");
  assert.equal(formatGap(10 * DAY + 210_000), "approximately 10 days");
  assert.equal(formatGap(DAY - 20_000), "approximately 1 day");
});

test("receipt line follows the incoming-event-time format", () => {
  assert.equal(
    receiptLine("2026-09-08T03:01:30.412Z", "2026-09-08T03:00:00.412Z"),
    "[Received 2026-09-08 03:01:30 UTC; 1 minute 30 seconds since the previous incoming message]",
  );
  // The first incoming message has no previous one to measure from.
  assert.equal(
    receiptLine("2026-09-08T03:01:30Z", null),
    "[Received 2026-09-08 03:01:30 UTC]",
  );
  // A clock that stepped backward is said so, not rendered as a gap.
  assert.equal(
    receiptLine("2026-09-08T03:00:00Z", "2026-09-08T03:00:05Z"),
    "[Received 2026-09-08 03:00:00 UTC; the receipt clock reads 5 seconds earlier than for the previous incoming message]",
  );
  // A journal record from before receipts were pinned shows its journal
  // time, labelled as such, and no invented gap.
  assert.equal(
    receiptLine(undefined, undefined, "2026-09-17T04:56:58.100Z"),
    "[Recorded 2026-09-17 04:56:58 UTC]",
  );
  assert.equal(receiptLine(undefined, undefined, undefined), "");
});

test("a pre-receipt journal record renders its journal time, not a gap", () => {
  const m = eventMessage({
    persona_id: PERSONA,
    seq: 1,
    turn_id: "t-1",
    kind: "input_received",
    payload: {
      input_id: "in-1",
      kind: "message",
      text: "hello",
      actor_kind: "human",
      source_surface: "direct_chat",
    },
    created_at: "2026-09-17T04:56:58.100Z",
  });
  assert.equal(m?.content, "[Recorded 2026-09-17 04:56:58 UTC]\n[human] hello");
});

test("an unanswered request from a failed turn is marked, and a long gap is visible to the next model", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const t0 = Date.parse("2026-09-17T04:56:57.770Z");
  const later = Date.parse("2026-09-27T16:49:03.580Z");

  // Two messages arrive while no model connection is selected: both turns
  // fail before any model is consulted.
  arriveAll(state, [
    ["in-1", "hello", t0],
    ["in-2", "please gather information", t0 + 5_340],
  ]);
  const unbound = new Secretary(cfg(state, new NoSelectionProvider()));
  await unbound.start();
  assert.equal(await unbound.step(), "turn");
  assert.equal(await unbound.step(), "turn");
  await unbound.stop();
  const failed = state.outboxEntries.filter((e) => e.kind === "turn_failed");
  assert.equal(failed.length, 2);

  // Ten days later a model is selected, and the host has restarted.
  arrive(state, "in-3", "long time no see", later);
  const provider = new RecordingProvider("model-a");
  const s = new Secretary(cfg(state, provider));
  await s.start();
  assert.equal(await s.step(), "turn");
  await s.stop();

  const sent = provider.requests[0]!;
  const view = journalView(sent).map((m) => m.content);
  assert.deepEqual(view, [
    "[Received 2026-09-17 04:56:57 UTC]\n[human] hello",
    "[turn failed: no model connection was selected — this request stopped here without finishing normally. What is recorded above for it happened as shown — for example, a message you sent stays sent; nothing further runs for it]",
    "[Received 2026-09-17 04:57:03 UTC; approximately 5 seconds since the previous incoming message]\n[human] please gather information",
    "[turn failed: no model connection was selected — this request stopped here without finishing normally. What is recorded above for it happened as shown — for example, a message you sent stays sent; nothing further runs for it]",
  ]);
  assert.equal(
    sent.messages.at(-1)?.content,
    "[Received 2026-09-27 16:49:03 UTC; approximately 10 days 12 hours since the previous incoming message]\n[human] long time no see",
  );
  // The current input is sent once, as the current message only.
  assert.equal(
    sent.messages.filter((m) => m.content.includes("long time no see")).length,
    1,
  );

  // The failed requests are history, not work: they stay resolved, no turn
  // serves them again, and nothing is left to run.
  for (const id of ["in-1", "in-2"]) {
    assert.equal(state.inputs.find((i) => i.input_id === id)?.status, "done");
  }
  assert.equal(provider.requests.length, 1);
  const again = new Secretary(cfg(state, provider));
  await again.start();
  assert.equal(await again.step(), "idle");
  await again.stop();
  assert.equal(provider.requests.length, 1);
  const journal = await state.events(PERSONA, 0);
  assert.deepEqual(
    journal.map((e) => e.kind),
    [
      "input_received",
      "turn_failed",
      "input_received",
      "turn_failed",
      "input_received",
      "assistant_message",
    ],
  );
});

test("a restarted secretary on a different model sees the same journal, with the same pinned times", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const t0 = Date.parse("2026-09-27T16:49:03.580Z");

  arrive(state, "in-1", "first", t0);
  const a = new RecordingProvider("model-a");
  const sa = new Secretary(cfg(state, a));
  await sa.start();
  assert.equal(await sa.step(), "turn");
  arrive(state, "in-2", "second", t0 + 34_290);
  assert.equal(await sa.step(), "turn");
  await sa.stop();

  // The model changes and the host restarts; the next message arrives a
  // day later.
  arrive(state, "in-3", "third", t0 + DAY);
  const b = new RecordingProvider("model-b");
  const sb = new Secretary(cfg(state, b));
  await sb.start();
  assert.equal(await sb.step(), "turn");
  await sb.stop();

  // What model-a saw as the current message is exactly what model-b sees
  // in the journal: the receipt was fixed at admission, not re-measured.
  const secondAsCurrent = a.requests[1]!.messages.at(-1)!;
  const viewB = journalView(b.requests[0]!);
  assert.deepEqual(viewB, [
    ...journalView(a.requests[1]!),
    secondAsCurrent,
    { role: "assistant", content: "reply from model-a" },
  ]);
  assert.equal(
    secondAsCurrent.content,
    "[Received 2026-09-27 16:49:37 UTC; approximately 34 seconds since the previous incoming message]\n[human] second",
  );
  assert.equal(
    b.requests[0]!.messages.at(-1)?.content,
    "[Received 2026-09-28 16:49:03 UTC; approximately 23 hours 59 minutes since the previous incoming message]\n[human] third",
  );
});

/** A store that deterministically rejects (400) every commit whose events
 * include one of `rejectKinds` — the persistence-rejection paths that send
 * commitTurnFinal down its scrubbed and minimal tiers. */
class RejectingState extends FakeState {
  readonly sent: CommitRequest[] = [];
  readonly rejectKinds: string[];
  healed = false;
  rejectFirst = 0;
  constructor(rejectKinds: string[]) {
    super();
    this.rejectKinds = rejectKinds;
  }
  override async commitTurn(
    persona: string,
    turnId: string,
    generation: number,
    req: CommitRequest,
  ) {
    if (this.healed) return super.commitTurn(persona, turnId, generation, req);
    this.sent.push(structuredClone(req));
    if (
      this.sent.length <= this.rejectFirst ||
      req.outcome === "complete" ||
      req.events.some((e) => this.rejectKinds.includes(e.kind))
    ) {
      throw new StateError(400, "commit contains a NUL byte jsonb cannot store");
    }
    return super.commitTurn(persona, turnId, generation, req);
  }
}

const markers = (req: CommitRequest) =>
  req.events.filter((e) => e.kind === "turn_failed");

/** Serve in-1 through `provider` against `state`, then let a fresh
 * secretary serve in-2 and return what that next model was shown. */
async function failThenContinue(
  state: RejectingState,
  provider: ModelProvider,
): Promise<string[]> {
  const t0 = Date.parse("2026-09-20T09:00:00Z");
  state.addPersona(PERSONA);
  arrive(state, "in-1", "book the usual table", t0);
  const first = new Secretary(cfg(state, provider));
  await first.start();
  assert.equal(await first.step(), "turn");
  // Replaying the stored tier is the identical request: no conflict, and
  // the journal still holds exactly one marker.
  const turn = [...state.turns.values()].find((t) => t.input_id === "in-1")!;
  state.healed = true;
  await state.commitTurn(PERSONA, turn.turn_id, turn.generation, state.sent.at(-1)!);
  state.healed = false;
  const journal = await state.events(PERSONA, 0);
  assert.equal(journal.filter((e) => e.kind === "turn_failed").length, 1);
  await first.stop();
  assert.equal(state.inputs.find((i) => i.input_id === "in-1")?.status, "done");
  assert.deepEqual(
    state.outboxEntries.map((e) => e.kind),
    ["turn_failed"],
    "the requester is told the turn failed",
  );
  // Every tier sent at most one marker — scrubbing never adds a second.
  for (const req of state.sent) assert.ok(markers(req).length <= 1);

  // The store heals; the next message goes to a working model.
  state.healed = true;
  arrive(state, "in-2", "any news?", t0 + 3_600_000);
  const next = new RecordingProvider("model-b");
  const s = new Secretary(cfg(state, next));
  await s.start();
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle", "the failed request is not served again");
  await s.stop();
  assert.equal(next.requests.length, 1);
  const sent = next.requests[0]!;
  assert.equal(
    sent.messages.at(-1)?.content,
    "[Received 2026-09-20 10:00:00 UTC; 1 hour since the previous incoming message]\n[human] any news?",
  );
  return journalView(sent).map((m) => m.content);
}

test("a reply the store rejects is downgraded to a marked failure the next model sees", async () => {
  // Only the complete commit is rejected: the scrubbed tier stores the
  // downgraded failure with its events and one marker.
  const state = new RejectingState([]);
  const view = await failThenContinue(state, new RecordingProvider("model-a"));
  assert.deepEqual(state.sent.map((r) => [r.outcome, markers(r).length]), [
    ["complete", 0],
    ["fail", 1],
  ]);
  assert.deepEqual(view, [
    "[Received 2026-09-20 09:00:00 UTC]\n[human] book the usual table",
    "reply from model-a",
    "[turn failed — this request stopped here without finishing normally. What is recorded above for it happened as shown — for example, a message you sent stays sent; nothing further runs for it]",
  ]);
});

test("a downgraded reply whose events cannot be stored at all still leaves a marker", async () => {
  const state = new RejectingState(["assistant_message"]);
  const view = await failThenContinue(state, new RecordingProvider("model-a"));
  const minimal = state.sent.at(-1)!;
  assert.equal(state.sent.length, 3);
  assert.equal(minimal.retryable, false);
  assert.deepEqual(minimal.events, [
    { kind: "turn_failed", payload: { error_kind: null, record_lost: true } },
  ]);
  assert.deepEqual(view, [
    "[turn failed — this turn's closing records could not be stored. Tool calls it ran are recorded above with their results as they happened — for example, a message you sent stays sent — but any reply it ended with is not shown here]",
  ]);
});

test("a terminal failure whose events cannot be stored keeps its marker and cause in the minimal tier", async () => {
  // No model selected: the turn fails terminally before any model call,
  // and the store refuses every commit carrying the input receipt.
  const state = new RejectingState(["input_received"]);
  const view = await failThenContinue(state, new NoSelectionProvider());
  assert.deepEqual(state.sent.map((r) => [r.retryable, markers(r).length]), [
    [false, 1],
    [false, 1],
    [false, 1],
  ]);
  assert.deepEqual(state.sent.at(-1)!.events, [
    {
      kind: "turn_failed",
      payload: { error_kind: "no_model_connection", record_lost: true },
    },
  ]);
  assert.deepEqual(view, [
    "[turn failed: no model connection was selected — this turn's closing records could not be stored. Tool calls it ran are recorded above with their results as they happened — for example, a message you sent stays sent — but any reply it ended with is not shown here]",
  ]);
});

test("a retryable failure stays unmarked through the scrubbed and minimal tiers", async () => {
  const state = new RejectingState([]);
  state.rejectFirst = 2;
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-1", "hi");
  const flaky: ModelProvider = {
    name: "flaky",
    async *stream() {
      throw new Error("provider exploded");
    },
  };
  const s = new Secretary(cfg(state, flaky));
  await s.start();
  assert.equal(await s.step(), "turn");
  await s.stop();
  assert.deepEqual(state.sent.map((r) => [r.retryable, markers(r).length]), [
    [true, 0],
    [true, 0],
    [true, 0],
  ]);
  assert.equal(state.inputs.find((i) => i.input_id === "in-1")?.status, "queued");
  assert.deepEqual(await state.events(PERSONA, 0), []);
});
