import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { COMPACT_L1_PROMPT, renderJournalContext } from "../src/memory.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { toolSpecs } from "../src/tools.ts";

const PERSONA = "01930e00-0000-7000-8000-0000000000a1";
/** ~11k estimated tokens per message: each exchange clears the 10k seal. */
const PAD = "x".repeat(44 * 1024);

/**
 * Turns get a short acknowledgement. A memory branch (turnId memory-l1-N)
 * waits for its gate, then answers with a replacement naming what the human
 * said in the target — or, when told to, tries a tool call instead.
 */
class MemoryProvider implements ModelProvider {
  readonly name = "memory-scripted";
  requests: ModelRequest[] = [];
  branchToolCall = false;
  private gates = new Map<
    number,
    { promise: Promise<void>; open: () => void }
  >();

  gate(chunk: number): void {
    let open = () => {};
    const promise = new Promise<void>((res) => {
      open = res;
    });
    this.gates.set(chunk, { promise, open });
  }

  release(chunk: number): void {
    this.gates.get(chunk)?.open();
  }

  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(req);
    const last = req.messages[req.messages.length - 1]?.content ?? "";
    const branch = /^memory-l1-(\d+)$/.exec(req.turnId);
    if (!branch) {
      yield { type: "text", delta: `ok ${last.slice(0, 24)}` };
      yield { type: "done", usage: {} };
      return;
    }
    const chunk = Number(branch[1]);
    const gate = this.gates.get(chunk);
    if (gate) {
      await new Promise<void>((res, rej) => {
        const abort = () => rej(new DOMException("aborted", "AbortError"));
        if (req.signal?.aborted) abort();
        req.signal?.addEventListener("abort", abort, { once: true });
        void gate.promise.then(res);
      });
    }
    if (this.branchToolCall) {
      yield {
        type: "tool_call",
        call: { id: "b-0", name: "journal.note", arguments: { text: "x" } },
      };
      yield { type: "done", usage: {} };
      return;
    }
    const target = JSON.parse(
      last.slice(last.indexOf("compact_target\n") + "compact_target\n".length),
    ) as { events: { kind: string; payload: Record<string, unknown> }[] };
    const said = target.events
      .filter((e) => e.kind === "input_received")
      .map((e) => String(e.payload.text ?? "").slice(0, 24));
    yield {
      type: "text",
      delta: `L1 chunk ${chunk}: the human said ${said.join(" / ")}`,
    };
    yield { type: "done", usage: {} };
  }
}

function cfg(state: FakeState, provider: ModelProvider): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: "memory-test",
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

async function settle(s: Secretary): Promise<void> {
  for (let i = 0; i < 2_000 && s.memoryBusy; i++) {
    await new Promise((r) => setTimeout(r, 2));
  }
  assert.equal(s.memoryBusy, false, "memory branch did not settle");
}

test("memory: a correction during preparation survives; the replacement renders in place", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new MemoryProvider();
  provider.gate(1);
  const s = new Secretary(cfg(state, provider));
  await s.start();
  const turn = async (id: string, text: string) => {
    state.addInput(PERSONA, id, text);
    assert.equal(await s.step(), "turn");
  };
  const lastTurn = () =>
    [...provider.requests]
      .reverse()
      .find((r) => !r.turnId.startsWith("memory-l1-"))!;
  const chunk1 = () => state.memoryChunks.find((c) => c.chunk_seq === 1)!;

  await turn("m1", `COLOR=amber ${PAD}`);
  await turn("m2", `second topic ${PAD}`);
  // The next step seals [m1] at m2's input boundary and starts the branch;
  // the conversation is not blocked by it.
  assert.equal(await s.step(), "idle");
  assert.equal(s.memoryBusy, true);
  assert.equal(chunk1().status, "preparing");

  await turn("m3", "CORRECTION=violet, not amber");
  const during = lastTurn().messages.map((m) => m.content);
  assert.ok(
    during.some((c) => /^\[\w+\] COLOR=amber/.test(c)),
    "the raw original stays in context while its chunk is prepared",
  );

  // The branch keeps the parent's own prefix: same system prompt and tool
  // definitions, with the instruction and target appended at the end.
  const branch = provider.requests.find((r) => r.turnId === "memory-l1-1")!;
  assert.equal(branch.messages[0]!.content, lastTurn().messages[0]!.content);
  assert.deepEqual(
    branch.tools.map((t) => t.name),
    toolSpecs().map((t) => t.name),
  );
  const tail = branch.messages[branch.messages.length - 1]!.content;
  assert.ok(
    tail.startsWith(COMPACT_L1_PROMPT) && tail.includes("compact_target"),
  );

  provider.release(1);
  await settle(s);
  // Shelved, not applied: the live raw estimate is still under 40k.
  assert.equal(chunk1().status, "prepared");

  await turn("m4", `fourth ${PAD}`);
  await turn("m5", `fifth ${PAD}`);
  // m6's step sees ~44k live raw and applies the shelved chunk before the
  // turn loads its context.
  await turn("m6", "which color do I like now?");
  assert.equal(chunk1().status, "applied");

  const after = lastTurn().messages.map((m) => m.content);
  const frag = after.findIndex(
    (c) =>
      c.startsWith("[Memory fragment") &&
      c.includes("L1 chunk 1: the human said COLOR=amber"),
  );
  const m2 = after.findIndex((c) => /^\[\w+\] second topic/.test(c));
  const corr = after.findIndex((c) => /^\[\w+\] CORRECTION=violet/.test(c));
  assert.ok(
    frag > 0 && frag < m2 && m2 < corr,
    JSON.stringify({ frag, m2, corr }),
  );
  assert.ok(!after.some((c) => /^\[\w+\] COLOR=amber/.test(c)));

  // Application changes the sent context only; the journal keeps the original.
  const evs = await state.events(PERSONA, 0);
  assert.ok(
    evs.some(
      (e) =>
        e.kind === "input_received" &&
        String(e.payload.text).startsWith("COLOR=amber"),
    ),
  );
  await s.stop();
});

test("memory: a branch that tries to act instead of replacing is not adopted", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new MemoryProvider();
  provider.branchToolCall = true;
  const s = new Secretary(cfg(state, provider));
  await s.start();
  state.addInput(PERSONA, "m1", `first ${PAD}`);
  assert.equal(await s.step(), "turn");
  state.addInput(PERSONA, "m2", `second ${PAD}`);
  assert.equal(await s.step(), "turn");
  assert.equal(await s.step(), "idle");
  await settle(s);

  const c = state.memoryChunks[0]!;
  assert.equal(c.status, "sealed");
  assert.equal(c.attempts, 1);
  assert.match(c.last_error ?? "", /tool call/);
  assert.ok(c.not_before, "retry is paced by backoff");
  const evs = await state.events(PERSONA, 0);
  assert.ok(!evs.some((e) => e.kind === "note"), "the branch's call never ran");
  await s.stop();
});

test("memory: raw records outside the send cap are marked once, in order", () => {
  const ev = (seq: number, text: string) => ({
    persona_id: PERSONA,
    seq,
    turn_id: "t",
    kind: "assistant_message",
    payload: { text },
    created_at: "2026-09-14T00:00:00Z",
  });
  const block = (chunk_seq: number, first_seq: number, last_seq: number) => ({
    chunk_seq,
    layer: 1,
    first_seq,
    last_seq,
    text: `block ${chunk_seq}`,
    est_tokens: 2,
  });
  // Journal: block 1 [1-4], omitted raw [5-9], block 2 [10-12], raw 13-14.
  const out = renderJournalContext(
    [ev(13, "thirteen"), ev(14, "fourteen")],
    [block(1, 1, 4), block(2, 10, 12)],
    {
      count: 5,
      first_seq: 5,
      last_seq: 9,
      first_time: "2026-09-13T00:00:00Z",
      last_time: "2026-09-13T01:00:00Z",
    },
  ).map((m) => m.content);
  assert.equal(out.length, 5);
  assert.match(out[0]!, /block 1$/);
  assert.match(out[1]!, /^\[5 earlier records — journal seq 5 through 9/);
  assert.match(out[1]!, /"from_seq":5/);
  assert.match(out[2]!, /block 2$/);
  assert.deepEqual(out.slice(3), ["thirteen", "fourteen"]);
});
