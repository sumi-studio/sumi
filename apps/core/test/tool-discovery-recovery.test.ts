import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import type { ModelEvent, ModelProvider, ModelRequest } from "../src/provider.ts";
import { Secretary } from "../src/secretary.ts";
import { toolSpecs } from "../src/tools.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000001";

class ObservingProvider implements ModelProvider {
  readonly name = "discovery-observer";
  readonly requests: ModelRequest[] = [];

  async *stream(request: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(request);
    yield { type: "text", delta: "Observed available tools." };
    yield { type: "done", usage: {} };
  }
}

function secretary(state: FakeState, provider: ModelProvider): Secretary {
  return new Secretary({
    personaId: PERSONA,
    holderId: "discovery-recovery-test",
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 20,
    pollIntervalMs: 1,
    scheduleEveryMs: 0,
    idgen: () => crypto.randomUUID(),
  });
}

test("a running secretary discovers delegated tools after a temporary discovery failure", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ObservingProvider();
  let discoveryCalls = 0;
  state.listTools = async () => {
    discoveryCalls++;
    if (discoveryCalls === 1) throw new TypeError("temporary network failure");
    return [...toolSpecs().map((tool) => tool.name), "file.stat"];
  };
  const life = secretary(state, provider);
  await life.start();
  try {
    for (let turn = 0; turn < 3; turn++) {
      state.addInput(PERSONA, `input-${turn}`, "What tools can you use?");
      assert.equal(await life.step(), "turn");
    }
    const offers = provider.requests.map((request) =>
      request.tools.map((tool) => tool.name),
    );
    assert.equal(offers.length, 3);
    assert.ok(!offers[0]!.includes("file.stat"), "unconfirmed tools stay withheld");
    assert.ok(offers[1]!.includes("file.stat"), "discovery recovers without replacing the secretary");
    assert.deepEqual(offers[2], offers[1], "successful discovery remains stable");
    assert.equal(discoveryCalls, 2, "only failed discovery is retried");
  } finally {
    await life.stop();
  }
});

test("a successful empty tool list is authoritative, not a discovery failure", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new ObservingProvider();
  let discoveryCalls = 0;
  state.listTools = async () => {
    discoveryCalls++;
    return [];
  };
  const life = secretary(state, provider);
  await life.start();
  try {
    for (let turn = 0; turn < 2; turn++) {
      state.addInput(PERSONA, `input-${turn}`, "Hello.");
      assert.equal(await life.step(), "turn");
    }
    assert.equal(provider.requests.length, 2);
    assert.ok(provider.requests.every((request) => request.tools.length === 0));
    assert.equal(discoveryCalls, 1);
  } finally {
    await life.stop();
  }
});
