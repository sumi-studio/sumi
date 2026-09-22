import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";

// Parity check for the terminal tool surface against the Go store
// semantics: acceptance is 'intended' (not delivery), and
// 'terminal.inputs' reads the same durable ledger the person's
// GET /terminal/inputs route serves.

const PA = "01900000-0000-7000-8000-000000000001";

// One tool call per turn: plan round 0 carries exactly the call we then
// claim at index 0 (Go plan-binding requires the claim to equal the
// recorded call).
async function callTool(
  s: FakeState,
  tool: string,
  request: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  s.addInput(PA, `in-${crypto.randomUUID()}`, "run a terminal tool");
  const g = (await s.acquireWriter(PA, "h", 60_000)).generation;
  const turn = await s.loadTurn(PA, g, `t-${crypto.randomUUID()}`, 50);
  const turnId = turn.turn!.turn_id;
  await s.savePlan(PA, g, {
    turnId,
    round: 0,
    text: "",
    calls: [{ tool, route: "normal", request }],
    usage: {},
  });
  const op = await s.claimOperation(PA, g, {
    operationId: `op-${crypto.randomUUID()}`,
    turnId,
    tool,
    callIndex: 0,
    request,
  });
  const response = op.operation.response as Record<string, unknown>;
  await s.commitTurn(PA, turnId, g, {
    outcome: "complete",
    events: [],
    output: { text: "" },
  });
  return response;
}

test("fake-state: terminal.inputs reads the same ledger the person sees", async () => {
  const s = new FakeState();
  s.addPersona(PA);
  s.addInput(PA, "in-1", "open a shell");

  const opened = await callTool(s, "terminal.open", { name: "sh" });
  const session = opened.session as { session_id: string; status: string };
  assert.equal(session.status, "active");

  // Acceptance is 'intended' — durable queueing, never a write receipt.
  const written = await callTool(s, "terminal.write", {
    session_id: session.session_id,
    data: "ls -la\n",
  });
  const receipt = written.input as { input_id: string; seq: number; status: string };
  assert.equal(receipt.status, "intended");
  assert.ok(typeof receipt.input_id === "string");
  assert.equal(receipt.seq, 1);

  // The secretary's ledger read mirrors GET /terminal/inputs.
  const ledger = await callTool(s, "terminal.inputs", {
    session_id: session.session_id,
  });
  const inputs = ledger.inputs as {
    input_id: string; seq: number; kind: string; status: string;
  }[];
  assert.equal(inputs.length, 1);
  assert.equal(inputs[0]!.input_id, receipt.input_id);
  assert.equal(inputs[0]!.kind, "stdin");
  assert.equal(inputs[0]!.status, "intended");

  // after_seq is an incremental cursor.
  const empty = await callTool(s, "terminal.inputs", {
    session_id: session.session_id,
    after_seq: 1,
  });
  assert.deepEqual(empty.inputs, []);
});
