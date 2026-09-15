import assert from "node:assert/strict";
import http from "node:http";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  opencodeSessionHeader,
  SelectedModelProvider,
} from "../src/host/provider-env.ts";
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

type Seen = {
  url: string | undefined;
  auth: string | undefined;
  apiKeyHeader: string | undefined;
  versionHeader: string | undefined;
  extra: string | undefined;
  sessionHeader: string | undefined;
  model: string;
  outputBound: unknown;
  cacheKey: unknown;
};

async function withModelServer(
  fn: (baseUrl: string, seen: Seen[]) => Promise<void>,
): Promise<void> {
  const seen: Seen[] = [];
  const srv = http.createServer((req, res) => {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => {
      const parsed = JSON.parse(body) as Record<string, unknown>;
      const model = String(parsed.model);
      seen.push({
        url: req.url,
        auth: req.headers.authorization,
        apiKeyHeader: req.headers["x-api-key"] as string | undefined,
        versionHeader: req.headers["anthropic-version"] as string | undefined,
        extra: req.headers["x-fixture-tag"] as string | undefined,
        sessionHeader: req.headers["x-opencode-session"] as string | undefined,
        model: String(parsed.model),
        outputBound: parsed.max_tokens ?? parsed.max_output_tokens,
        cacheKey: parsed.prompt_cache_key,
      });
      res.writeHead(200, { "content-type": "text/event-stream" });
      const url = req.url ?? "";
      if (url.endsWith("/responses")) {
        res.end(
          [
            `data: ${JSON.stringify({ type: "response.output_text.delta", item_id: "m1", output_index: 0, content_index: 0, delta: `from ${model}` })}\n\n`,
            `data: ${JSON.stringify({ type: "response.completed", response: { status: "completed", usage: { input_tokens: 3, output_tokens: 2 }, output: [] } })}\n\n`,
          ].join(""),
        );
        return;
      }
      if (url.endsWith("/v1/messages")) {
        res.end(
          [
            `data: ${JSON.stringify({ type: "message_start", message: { usage: { input_tokens: 3 } } })}\n\n`,
            `data: ${JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: `from ${model}` } })}\n\n`,
            `data: ${JSON.stringify({ type: "message_delta", delta: { stop_reason: "end_turn" }, usage: { output_tokens: 2 } })}\n\n`,
            `data: ${JSON.stringify({ type: "message_stop" })}\n\n`,
          ].join(""),
        );
        return;
      }
      res.end(
        [
          JSON.stringify({
            choices: [{ delta: { content: `from ${model}` } }],
          }),
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
    const done = first.at(-1) as unknown as {
      usage: { model_binding: unknown };
    };
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
      {
        url: "/chat/completions",
        auth: "Bearer key-a",
        apiKeyHeader: undefined,
        versionHeader: undefined,
        extra: undefined,
        sessionHeader: undefined,
        model: "model-a",
        outputBound: undefined,
        cacheKey: undefined,
      },
      {
        url: "/chat/completions",
        auth: "Bearer key-b",
        apiKeyHeader: undefined,
        versionHeader: undefined,
        extra: undefined,
        sessionHeader: undefined,
        model: "model-b",
        outputBound: undefined,
        cacheKey: undefined,
      },
    ]);
    assert.equal(fallback.calls, 0);
  });
});

test("the selected preset picks the wire protocol and carries the connection's extra headers", async () => {
  await withModelServer(async (baseUrl, seen) => {
    const state = new FakeState();
    const fallback = new Fallback();
    const p = await metered(state, fallback);

    for (const [preset, url, authField] of [
      ["openai-chat", "/chat/completions", "auth"],
      ["openai-responses", "/responses", "auth"],
      ["anthropic", "/v1/messages", "apiKeyHeader"],
    ] as const) {
      state.setModelBinding(
        PERSONA,
        api(baseUrl, { preset, extra_headers: { "X-Fixture-Tag": preset } }),
      );
      const evs = await collect(p);
      assert.equal(
        evs
          .filter((e) => e.type === "text")
          .map((e) => e.delta)
          .join(""),
        "from model-a",
        preset,
      );
      const req = seen.at(-1)!;
      assert.equal(req.url, url, preset);
      // Exactly one auth mechanism per wire; the connection's extra
      // header arrived only on this connection's request.
      assert.equal(req.extra, preset, `${preset}: extra header`);
      if (authField === "auth") {
        assert.equal(req.auth, "Bearer key-a", preset);
        assert.equal(req.apiKeyHeader, undefined, preset);
      } else {
        assert.equal(req.apiKeyHeader, "key-a", preset);
        assert.equal(req.auth, undefined, preset);
        assert.equal(req.versionHeader, "2023-06-01", preset);
      }
    }
    assert.equal(seen.length, 3);
    assert.equal(fallback.calls, 0);
  });
});

test("a selection the core cannot honor fails the request without using another model", async () => {
  await withModelServer(async (baseUrl, seen) => {
    const cases: [string, ModelBinding][] = [
      ["接続しない", { selection: "none" }],
      ["chatgpt", { selection: "chatgpt", reason: "not served" }],
      ["unknown wire", api(baseUrl, { preset: "not-a-wire" })],
      [
        "no credential",
        { ...api(baseUrl), api_key: undefined, credential_available: false },
      ],
      [
        "no credential (anthropic)",
        {
          ...api(baseUrl, { preset: "anthropic" }),
          api_key: undefined,
          credential_available: false,
        },
      ],
      ["deleted connection", { selection: "api", reason: "gone" }],
    ];
    for (const [label, binding] of cases) {
      const state = new FakeState();
      const fallback = new Fallback();
      state.setModelBinding(PERSONA, binding);
      await assert.rejects(
        collect(selected(state, fallback)),
        (e: unknown) =>
          e instanceof ModelError &&
          !e.retryable &&
          // An unusable selection is an availability gap — distinguishable
          // from a genuine evaluated-model failure so callers that spend
          // model budget (memory attempts) can pause instead.
          e.unavailable === true,
        label,
      );
      assert.equal(fallback.calls, 0, `${label}: operator model not used`);
    }
    assert.equal(seen.length, 0, "no request reached any model");
  });
});

test("the connection's output bound reaches its wire; OpenCode carries the session header", async () => {
  await withModelServer(async (baseUrl, seen) => {
    const state = new FakeState();
    const p = await metered(state, new Fallback());

    // Anthropic requires the field — the configured bound overrides the
    // default budget.
    state.setModelBinding(
      PERSONA,
      api(baseUrl, { preset: "anthropic", max_output_tokens: 512 }),
    );
    await collect(p);
    assert.equal(seen.at(-1)!.outputBound, 512);

    // Responses: unconfigured omits the field; configured sends it; the
    // persona id feeds prompt_cache_key either way.
    state.setModelBinding(
      PERSONA,
      api(baseUrl, { preset: "openai-responses" }),
    );
    await collect(p);
    assert.equal(seen.at(-1)!.outputBound, undefined);
    assert.equal(seen.at(-1)!.cacheKey, PERSONA);
    state.setModelBinding(
      PERSONA,
      api(baseUrl, { preset: "openai-responses", max_output_tokens: 900 }),
    );
    await collect(p);
    assert.equal(seen.at(-1)!.outputBound, 900);

    // The chat wire sends no output bound; OpenCode presets carry the
    // persona's stable identity as the session header.
    state.setModelBinding(PERSONA, api(baseUrl, { preset: "opencode-go" }));
    await collect(p);
    assert.equal(seen.at(-1)!.outputBound, undefined);
    assert.equal(seen.at(-1)!.sessionHeader, PERSONA);
    state.setModelBinding(PERSONA, api(baseUrl, { preset: "openai-chat" }));
    await collect(p);
    assert.equal(seen.at(-1)!.sessionHeader, undefined);
  });
});

test("the OpenCode session header is chosen by preset or endpoint host", () => {
  // The Go gateway 400s (MissingSessionID) on every protocol without
  // x-opencode-session — verified live. Generic presets pointed at
  // opencode.ai get the per-persona header on any protocol; non-OpenCode
  // endpoints on generic presets never do.
  assert.equal(
    opencodeSessionHeader("openai-responses", "https://opencode.ai/zen/go/v1"),
    "x-opencode-session",
  );
  assert.equal(
    opencodeSessionHeader("anthropic", "https://opencode.ai/zen/go/v1"),
    "x-opencode-session",
  );
  assert.equal(
    opencodeSessionHeader("openai-chat", "https://api.openai.com/v1"),
    undefined,
  );
  assert.equal(
    opencodeSessionHeader("anthropic", "https://api.anthropic.com"),
    undefined,
  );
  assert.equal(
    opencodeSessionHeader("openai-responses", "http://127.0.0.1:8080"),
    undefined,
  );
  assert.equal(
    opencodeSessionHeader("opencode-go", "https://anything.example/v1"),
    "x-opencode-session",
  );
});

test("no selection uses the operator default; a lookup outage retries rather than guessing", async () => {
  const state = new FakeState();
  const fallback = new Fallback();
  const evs = await collect(await metered(state, fallback));
  assert.equal(fallback.calls, 1);
  assert.deepEqual(
    (evs.at(-1) as unknown as { usage: { model_binding: unknown } }).usage
      .model_binding,
    {
      selection: "unset",
      provider: "operator-default",
    },
  );

  const failing = (status: number) =>
    ({
      modelBinding: () => Promise.reject(new StateError(status, "lookup")),
    }) as unknown as StateClient;
  await assert.rejects(
    collect(selected(failing(503), fallback)),
    (e: unknown) =>
      e instanceof ModelError && e.retryable && e.unavailable === true,
  );
  await assert.rejects(
    collect(selected(failing(404), fallback)),
    (e: unknown) =>
      e instanceof ModelError && !e.retryable && e.unavailable === true,
  );
  assert.equal(
    fallback.calls,
    1,
    "the default is never a fallback for a failed lookup",
  );
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
    assert.equal(
      fallback.calls,
      0,
      "operator default never substitutes for carried intent",
    );
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
      (evs.at(-1) as unknown as { usage: { model_binding: unknown } }).usage
        .model_binding,
      {
        selection: "api",
        connection_id: "conn-1",
        preset: "openai-chat",
        model: "model-a",
        version: "v1",
      },
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
  await assert.rejects(
    async () => state.clearModelIntent(PERSONA),
    (e: unknown) => e instanceof StateError && e.status === 409,
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
  await assert.rejects(
    async () => state.clearModelIntent(PERSONA),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );
});

test("the operator env fallback validates its URL at construction", async () => {
  const { providerFromEnv } = await import("../src/host/provider-env.ts");
  const env = (over: Record<string, string | undefined>) => (name: string) =>
    over[name];
  // An unparseable base URL fails at boot, not as a per-request defect.
  assert.throws(
    () =>
      providerFromEnv(
        env({
          SUMI_MODEL_PROVIDER: "openai",
          SUMI_MODEL_BASE_URL: "not a url",
          SUMI_MODEL_API_KEY: "k",
          SUMI_MODEL_MODEL: "m",
        }),
      ),
    /SUMI_MODEL_BASE_URL is not a URL/,
  );
  // A well-formed env produces a working provider.
  const p = providerFromEnv(
    env({
      SUMI_MODEL_PROVIDER: "openai",
      SUMI_MODEL_BASE_URL: "http://127.0.0.1:1",
      SUMI_MODEL_API_KEY: "k",
      SUMI_MODEL_MODEL: "m",
    }),
  );
  assert.equal(p.name, "openai");
});
