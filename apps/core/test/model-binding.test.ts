import assert from "node:assert/strict";
import http from "node:http";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import {
  ModelError,
  type ModelEvent,
  type ModelProvider,
} from "../src/provider.ts";
import { type StateClient, StateError } from "../src/state-client.ts";
import type { ModelBinding } from "../src/types.ts";

/**
 * The user's selected model connection drives every consultation: the real
 * OpenAIProvider against a loopback chat-completions stub, with the binding
 * served by the state contract.
 */

const PERSONA = "01930e00-0000-7000-8000-0000000000c1";

const REQ = {
  personaId: PERSONA,
  turnId: "t",
  generation: 1,
  round: 0,
  messages: [{ role: "user" as const, content: "hi" }],
  tools: [],
};

class Fallback implements ModelProvider {
  readonly name = "operator-default";
  calls = 0;
  async *stream(): AsyncIterable<ModelEvent> {
    this.calls++;
    yield { type: "text", delta: "operator" };
    yield { type: "done", usage: {} };
  }
}

type Seen = { auth: string | undefined; model: string };

async function withModelServer(
  fn: (baseUrl: string, seen: Seen[]) => Promise<void>,
): Promise<void> {
  const seen: Seen[] = [];
  const srv = http.createServer((req, res) => {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => {
      const model = String((JSON.parse(body) as { model: unknown }).model);
      seen.push({ auth: req.headers.authorization, model });
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.end(
        [
          JSON.stringify({ choices: [{ delta: { content: `from ${model}` } }] }),
          JSON.stringify({ choices: [{ delta: {}, finish_reason: "stop" }] }),
          "[DONE]",
        ]
          .map((l) => `data: ${l}\n\n`)
          .join(""),
      );
    });
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const port = (srv.address() as { port: number }).port;
  try {
    await fn(`http://127.0.0.1:${port}`, seen);
  } finally {
    await new Promise<void>((r) => srv.close(() => r()));
  }
}

function api(
  baseUrl: string,
  over: Partial<NonNullable<ModelBinding["connection"]>> = {},
  key = "key-a",
): ModelBinding {
  return {
    selection: "api",
    connection: {
      id: "conn-1",
      name: "Work",
      preset: "openai-chat",
      base_url: baseUrl,
      model: "model-a",
      version: "v1",
      ...over,
    },
    api_key: key,
    credential_available: true,
  };
}

async function collect(p: ModelProvider): Promise<ModelEvent[]> {
  const out: ModelEvent[] = [];
  for await (const ev of p.stream(REQ)) out.push(ev);
  return out;
}

function selected(state: StateClient, fallback: ModelProvider) {
  return new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback,
    timeoutMs: 5_000,
  });
}

// A metered call needs a live writer generation — hold one on the fake.
async function metered(state: FakeState, fallback: ModelProvider) {
  state.addPersona(PERSONA);
  const lease = await state.acquireWriter(PERSONA, "test-writer", 60_000);
  REQ.generation = lease.generation;
  return selected(state, fallback);
}

test("an API selection streams from exactly that connection and is re-read on every call", async () => {
  await withModelServer(async (baseUrl, seen) => {
    const state = new FakeState();
    const fallback = new Fallback();
    const p = await metered(state, fallback);

    state.setModelBinding(PERSONA, api(baseUrl));
    const first = await collect(p);
    assert.deepEqual(first[0], { type: "text", delta: "from model-a" });
    const done = first.at(-1) as unknown as { usage: { model_binding: unknown } };
    assert.deepEqual(done.usage.model_binding, {
      selection: "api",
      connection_id: "conn-1",
      preset: "openai-chat",
      model: "model-a",
      version: "v1",
    });

    // The user switches model and rotates the key: the next consultation
    // follows without a restart.
    state.setModelBinding(
      PERSONA,
      api(baseUrl, { model: "model-b", version: "v2" }, "key-b"),
    );
    const second = await collect(p);
    assert.deepEqual(second[0], { type: "text", delta: "from model-b" });

    assert.deepEqual(seen, [
      { auth: "Bearer key-a", model: "model-a" },
      { auth: "Bearer key-b", model: "model-b" },
    ]);
    assert.equal(fallback.calls, 0);
  });
});

test("a selection the core cannot honor fails the request without using another model", async () => {
  await withModelServer(async (baseUrl, seen) => {
    const cases: [string, ModelBinding][] = [
      ["接続しない", { selection: "none" }],
      ["chatgpt", { selection: "chatgpt", reason: "not served" }],
      ["anthropic wire", api(baseUrl, { preset: "anthropic" })],
      ["responses wire", api(baseUrl, { preset: "openai-responses" })],
      [
        "no credential",
        { ...api(baseUrl), api_key: undefined, credential_available: false },
      ],
      ["deleted connection", { selection: "api", reason: "gone" }],
    ];
    for (const [label, binding] of cases) {
      const state = new FakeState();
      const fallback = new Fallback();
      state.setModelBinding(PERSONA, binding);
      await assert.rejects(
        collect(selected(state, fallback)),
        (e: unknown) => e instanceof ModelError && !e.retryable,
        label,
      );
      assert.equal(fallback.calls, 0, `${label}: operator model not used`);
    }
    assert.equal(seen.length, 0, "no request reached any model");
  });
});

test("no selection uses the operator default; a lookup outage retries rather than guessing", async () => {
  const state = new FakeState();
  const fallback = new Fallback();
  const evs = await collect(await metered(state, fallback));
  assert.equal(fallback.calls, 1);
  assert.deepEqual((evs.at(-1) as unknown as { usage: { model_binding: unknown } }).usage.model_binding, {
    selection: "unset",
    provider: "operator-default",
  });

  const failing = (status: number) =>
    ({
      modelBinding: () => Promise.reject(new StateError(status, "lookup")),
    }) as unknown as StateClient;
  await assert.rejects(
    collect(selected(failing(503), fallback)),
    (e: unknown) => e instanceof ModelError && e.retryable,
  );
  await assert.rejects(
    collect(selected(failing(404), fallback)),
    (e: unknown) => e instanceof ModelError && !e.retryable,
  );
  assert.equal(fallback.calls, 1, "the default is never a fallback for a failed lookup");
});

test("a carried model intent blocks model calls until the destination binds", async () => {
  await withModelServer(async (baseUrl, seen) => {
    const state = new FakeState();
    state.addPersona(PERSONA);
    state.setModelIntent(PERSONA, {
      kind: "api",
      connection: { preset: "openai-chat", model: "model-9" },
    });
    const fallback = new Fallback();
    // The intent is unsatisfied: the env default is not an answer.
    await assert.rejects(
      collect(selected(state, fallback)),
      (e: unknown) => e instanceof ModelError && !e.retryable,
    );
    assert.equal(fallback.calls, 0, "operator default never substitutes for carried intent");
    assert.equal(seen.length, 0, "no request reached any model");
    const b = await state.modelBinding(PERSONA);
    assert.equal(b.selection, "needs_rebinding");
    assert.equal(b.intent?.kind, "api");

    // The destination's bound human selects a matching connection — the
    // test's explicit binding stands in for that selection — and the
    // carried intent no longer blocks.
    state.setModelBinding(PERSONA, api(baseUrl));
    const evs = await collect(await metered(state, fallback));
    assert.equal(seen.length, 1);
    assert.equal(seen[0]?.model, "model-a");
    assert.deepEqual(
      (evs.at(-1) as unknown as { usage: { model_binding: unknown } }).usage.model_binding,
      { selection: "api", connection_id: "conn-1", preset: "openai-chat", model: "model-a", version: "v1" },
    );

    // Clearing the intent restores ordinary unset semantics — the
    // operator's explicit fresh start, not a silent fallback.
    const cleared = new FakeState();
    cleared.addPersona(PERSONA);
    cleared.setModelIntent(PERSONA, null);
    const fb2 = new Fallback();
    await collect(await metered(cleared, fb2));
    assert.equal(fb2.calls, 1);
  });
});

test("the intent clear is fenced to staged and active personas", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.setModelIntent(PERSONA, { kind: "none" });
  // The intent is part of the sealed cut: clearing under seal would strip
  // what the next export ships — refused like the Go store.
  state.setPersonaAuthority(PERSONA, "sealed");
  await assert.rejects(async () => state.clearModelIntent(PERSONA), (e: unknown) =>
    e instanceof StateError && e.status === 409,
  );
  assert.equal(state.personas.get(PERSONA)?.model_intent?.kind, "none");
  // staged and active personas may clear — the destination escape.
  state.setPersonaAuthority(PERSONA, "staged");
  state.clearModelIntent(PERSONA);
  assert.equal(state.personas.get(PERSONA)?.model_intent, null);
  state.setModelIntent(PERSONA, { kind: "api" });
  state.setPersonaAuthority(PERSONA, "active");
  state.clearModelIntent(PERSONA);
  assert.equal(state.personas.get(PERSONA)?.model_intent, null);
  // A transferred persona is no longer this placement's to edit.
  state.setModelIntent(PERSONA, { kind: "none" });
  state.setPersonaAuthority(PERSONA, "transferred");
  await assert.rejects(async () => state.clearModelIntent(PERSONA), (e: unknown) =>
    e instanceof StateError && e.status === 409,
  );
});
