import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import http from "node:http";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import {
  type ChatMessage,
  ModelError,
  type ModelEvent,
  type ModelProvider,
  type ModelRequest,
} from "../src/provider.ts";
import { uuidV5 } from "../src/providers/chatgpt-codex.ts";
import type { ModelBinding } from "../src/types.ts";

/**
 * A ChatGPT subscription connection drives the real Responses adapter
 * against a loopback stand-in for chatgpt.com/backend-api/codex speaking
 * the Codex backend's wire (lite request shape, item-event streams whose
 * terminal response omits the output copy, 401/429 bodies).
 */

const PA = "01930e00-0000-7000-8000-0000000000d1";
const PB = "01930e00-0000-7000-8000-0000000000d2";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
function nth<T>(xs: readonly T[], i: number): T {
  const v = xs[i];
  assert.ok(v !== undefined, `missing item ${i}`);
  return v;
}

type Seen = {
  headers: http.IncomingHttpHeaders;
  body: Record<string, unknown>;
};
type Reply = (seen: Seen, res: http.ServerResponse) => void;

const sse = (events: unknown[]) =>
  events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");

/** A Codex-backend-shaped stream: text, one namespaced function call. */
const toolRound: Reply = (_seen, res) => {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end(
    sse([
      {
        type: "response.created",
        response: { id: "resp_1", status: "in_progress" },
      },
      {
        type: "response.output_item.added",
        output_index: 0,
        item: { type: "message", id: "msg_o1", role: "assistant", content: [] },
      },
      {
        type: "response.output_text.delta",
        item_id: "msg_o1",
        output_index: 0,
        content_index: 0,
        delta: "調べます",
      },
      {
        type: "response.output_item.done",
        output_index: 0,
        item: {
          type: "message",
          id: "msg_o1",
          role: "assistant",
          content: [{ type: "output_text", text: "調べます" }],
        },
      },
      {
        type: "response.output_item.added",
        output_index: 1,
        item: {
          type: "function_call",
          id: "fc_1",
          call_id: "call_abc",
          name: "remember",
          namespace: "functions",
          arguments: "",
        },
      },
      {
        type: "response.function_call_arguments.delta",
        item_id: "fc_1",
        output_index: 1,
        delta: '{"input":{"note"',
      },
      {
        type: "response.function_call_arguments.delta",
        item_id: "fc_1",
        output_index: 1,
        delta: ':"milk"},"route":"normal"}',
      },
      {
        type: "response.output_item.done",
        output_index: 1,
        item: {
          type: "function_call",
          id: "fc_1",
          call_id: "call_abc",
          name: "remember",
          namespace: "functions",
          arguments: '{"input":{"note":"milk"},"route":"normal"}',
        },
      },
      // The Codex backend does not repeat items in the terminal response.
      {
        type: "response.completed",
        response: {
          id: "resp_1",
          status: "completed",
          output: [],
          usage: {
            input_tokens: 120,
            output_tokens: 14,
            input_tokens_details: { cached_tokens: 100 },
          },
        },
      },
    ]),
  );
};

const textOnly: Reply = (_seen, res) => {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end(
    sse([
      {
        type: "response.output_text.delta",
        item_id: "m",
        output_index: 0,
        delta: "done",
      },
      {
        type: "response.completed",
        response: {
          status: "completed",
          output: [],
          usage: { input_tokens: 5, output_tokens: 1 },
        },
      },
    ]),
  );
};

async function withBackend(
  replies: Reply[],
  fn: (baseUrl: string, seen: Seen[]) => Promise<void>,
) {
  const seen: Seen[] = [];
  const srv = http.createServer((req, res) => {
    let raw = "";
    req.on("data", (d) => (raw += d));
    req.on("end", () => {
      const s = {
        headers: req.headers,
        body: JSON.parse(raw) as Record<string, unknown>,
      };
      seen.push(s);
      const reply = replies[Math.min(seen.length - 1, replies.length - 1)];
      reply?.(s, res);
    });
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const port = (srv.address() as { port: number }).port;
  try {
    await fn(`http://127.0.0.1:${port}/backend-api/codex`, seen);
  } finally {
    srv.closeAllConnections();
    await new Promise<void>((r) => srv.close(() => r()));
  }
}

function chatgpt(
  baseUrl: string,
  token: string,
  over: Partial<NonNullable<ModelBinding["connection"]>> = {},
  extra: Partial<ModelBinding> = {},
): ModelBinding {
  return {
    selection: "api",
    connection: {
      id: "conn-chatgpt",
      name: "ChatGPT",
      preset: "chatgpt-codex",
      base_url: baseUrl,
      model: "gpt-6-astra",
      version: "v1",
      account_id: "acct-1",
      reasoning_effort: "medium",
      ...over,
    },
    api_key: token,
    credential_available: true,
    ...extra,
  };
}

const TOOL = {
  name: "remember",
  description: "Save a note",
  parameters: {
    type: "object",
    properties: { note: { type: "string" } },
    required: ["note"],
  },
};

async function setup(personas: string[] = [PA]) {
  const state = new FakeState();
  const gens = new Map<string, number>();
  for (const p of personas) {
    state.addPersona(p);
    gens.set(p, (await state.acquireWriter(p, "w", 60_000)).generation);
  }
  const provider = (persona: string) =>
    new SelectedModelProvider({
      state,
      persona,
      fallback: {
        name: "fallback",
        stream(): AsyncIterable<ModelEvent> {
          throw new Error("fallback must never run");
        },
      },
      timeoutMs: 5_000,
    });
  const req = (
    persona: string,
    messages: ChatMessage[],
    signal?: AbortSignal,
  ): ModelRequest => ({
    personaId: persona,
    turnId: "t1",
    generation: gens.get(persona),
    round: 0,
    messages,
    tools: [TOOL],
    signal,
  });
  return { state, provider, req };
}

async function collect(p: ModelProvider, r: ModelRequest) {
  const out: ModelEvent[] = [];
  for await (const ev of p.stream(r)) out.push(ev);
  return out;
}

const MESSAGES: ChatMessage[] = [
  { role: "system", content: "You are Sumi, the same secretary as yesterday." },
  { role: "user", content: "牛乳を買うのを覚えておいて" },
];

test("lite wire shape: headers, ordered prefix items, no bound, namespaced tool call parsed", async () => {
  await withBackend([toolRound], async (baseUrl, seen) => {
    const { state, provider, req } = await setup();
    state.setModelBinding(PA, chatgpt(baseUrl, "tok-1"));
    const events = await collect(provider(PA), req(PA, MESSAGES));
    assert.deepEqual(events.slice(0, 2), [
      { type: "text", delta: "調べます" },
      {
        type: "tool_call",
        call: {
          id: "call_abc",
          name: "remember",
          route: "normal",
          arguments: { note: "milk" },
        },
      },
    ]);
    const done = events.at(-1) as {
      type: "done";
      usage: Record<string, unknown>;
    };
    assert.equal(done.type, "done");
    assert.equal(done.usage.input_tokens, 120);
    assert.deepEqual(done.usage.model_binding, {
      selection: "api",
      connection_id: "conn-chatgpt",
      preset: "chatgpt-codex",
      model: "gpt-6-astra",
      version: "v1",
    });

    assert.equal(seen.length, 1);
    const { headers, body } = nth(seen, 0);
    assert.equal(headers.authorization, "Bearer tok-1");
    assert.equal(headers["chatgpt-account-id"], "acct-1");
    assert.equal(headers.originator, "sumi");
    assert.equal(headers.session_id, PA);
    assert.equal(headers["x-openai-internal-codex-responses-lite"], "true");
    assert.equal(headers.accept, "text/event-stream");
    assert.equal(body.model, "gpt-6-astra");
    assert.equal(body.store, false);
    assert.equal(body.stream, true);
    assert.equal(body.tool_choice, "auto");
    assert.equal(body.parallel_tool_calls, false);
    assert.deepEqual(body.reasoning, { effort: "medium" });
    assert.equal(body.prompt_cache_key, PA);
    for (const absent of ["instructions", "tools", "max_output_tokens"]) {
      assert.equal(absent in body, false, `${absent} must not be sent`);
    }
    const input = body.input as Record<string, unknown>[];
    assert.equal(input[0]?.type, "additional_tools");
    assert.equal(input[0]?.role, "developer");
    assert.match(String(input[0]?.id), /^at_[0-9a-f-]{36}$/);
    const ns = nth(nth(input, 0).tools as Record<string, unknown>[], 0);
    assert.equal(ns.type, "namespace");
    assert.equal(ns.name, "functions");
    const fn = nth(ns.tools as Record<string, unknown>[], 0);
    assert.equal(fn.type, "function");
    assert.equal(fn.name, "remember");
    assert.equal(fn.strict, false);
    assert.deepEqual(input[1], {
      type: "message",
      id: input[1]?.id,
      role: "developer",
      content: [{ type: "input_text", text: nth(MESSAGES, 0).content }],
    });
    assert.match(String(input[1]?.id), /^msg_[0-9a-f-]{36}$/);
    assert.deepEqual(input[2], {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: nth(MESSAGES, 1).content }],
    });
  });
});

test("tool round replay and stable prefix identity", async () => {
  await withBackend([textOnly], async (baseUrl, seen) => {
    const { state, provider, req } = await setup([PA, PB]);
    state.setModelBinding(PA, chatgpt(baseUrl, "tok-a"));
    state.setModelBinding(
      PB,
      chatgpt(baseUrl, "tok-b", { account_id: "acct-b" }),
    );
    const replay: ChatMessage[] = [
      ...MESSAGES,
      {
        role: "assistant",
        content: "調べます",
        toolCalls: [
          {
            id: "call_abc",
            name: "remember",
            route: "normal",
            arguments: { note: "milk" },
          },
        ],
      },
      { role: "tool", toolCallId: "call_abc", content: '{"ok":true}' },
    ];
    await collect(provider(PA), req(PA, replay));
    await collect(provider(PA), req(PA, replay));
    await collect(provider(PB), req(PB, replay));
    const input = nth(seen, 0).body.input as Record<string, unknown>[];
    assert.deepEqual(input.slice(3), [
      {
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: "調べます" }],
      },
      {
        type: "function_call",
        call_id: "call_abc",
        name: "remember",
        namespace: "functions",
        arguments: JSON.stringify({ route: "normal", input: { note: "milk" } }),
      },
      {
        type: "function_call_output",
        call_id: "call_abc",
        output: '{"ok":true}',
      },
    ]);
    const ids = (i: number) =>
      (nth(seen, i).body.input as Record<string, unknown>[])
        .slice(0, 2)
        .map((x) => x.id);
    assert.deepEqual(
      ids(0),
      ids(1),
      "same persona + payload = same prefix ids",
    );
    assert.notDeepEqual(ids(0), ids(2), "another persona has its own identity");
    // Each persona's call carries only its own person's grant.
    assert.equal(nth(seen, 0).headers.authorization, "Bearer tok-a");
    assert.equal(nth(seen, 2).headers.authorization, "Bearer tok-b");
    assert.equal(nth(seen, 2).headers["chatgpt-account-id"], "acct-b");
    assert.equal(
      await uuidV5("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "python.org"),
      "886313e1-3b8a-5372-9b90-0c9aee199e5d",
      "RFC 4122 v5 known answer (Python uuid docs)",
    );
  });
});

test("a standard (non-lite) model keeps top-level instructions and tools", async () => {
  await withBackend([textOnly], async (baseUrl, seen) => {
    const { state, provider, req } = await setup();
    state.setModelBinding(
      PA,
      chatgpt(baseUrl, "tok-1", { model: "gpt-5.5", reasoning_effort: "" }),
    );
    await collect(provider(PA), req(PA, MESSAGES));
    const { headers, body } = nth(seen, 0);
    assert.equal(headers["x-openai-internal-codex-responses-lite"], undefined);
    assert.equal(body.instructions, nth(MESSAGES, 0).content);
    assert.equal((body.tools as unknown[]).length, 1);
    assert.equal(body.parallel_tool_calls, true);
    assert.equal("reasoning" in body, false);
  });
});

const unauthorized: Reply = (_s, res) => {
  res.writeHead(401, { "content-type": "application/json" });
  res.end('{"error":{"message":"token expired","code":"token_expired"}}');
};

test("a 401 before output refreshes once through the state service and resends the identical body", async () => {
  await withBackend([unauthorized, toolRound], async (baseUrl, seen) => {
    const { state, provider, req } = await setup();
    state.setModelBinding(PA, chatgpt(baseUrl, "tok-old"));
    state.modelCredentialRefresh = () => chatgpt(baseUrl, "tok-new");
    const events = await collect(provider(PA), req(PA, MESSAGES));
    assert.equal(events.at(-1)?.type, "done");
    assert.deepEqual(state.modelCredentialRefreshCalls, [
      {
        persona: PA,
        connectionId: "conn-chatgpt",
        rejectedTokenSha256: sha("tok-old"),
      },
    ]);
    assert.equal(seen.length, 2);
    assert.equal(nth(seen, 0).headers.authorization, "Bearer tok-old");
    assert.equal(nth(seen, 1).headers.authorization, "Bearer tok-new");
    assert.deepEqual(nth(seen, 1).body, nth(seen, 0).body);
  });
});

test("a second 401, a revoked grant, or a changed selection end the call without another model", async () => {
  await withBackend([unauthorized], async (baseUrl, seen) => {
    const { state, provider, req } = await setup();
    state.setModelBinding(PA, chatgpt(baseUrl, "tok-old"));
    state.modelCredentialRefresh = () => chatgpt(baseUrl, "tok-new");
    await assert.rejects(
      collect(provider(PA), req(PA, MESSAGES)),
      (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, false);
        assert.equal(e.cause, "model_reconnect_required");
        return true;
      },
    );
    assert.equal(seen.length, 2, "never a third attempt");

    state.modelCredentialRefresh = () =>
      chatgpt(
        baseUrl,
        "",
        {},
        {
          credential_available: false,
          api_key: undefined,
          credential_state: "reconnect_required",
        },
      );
    await assert.rejects(
      collect(provider(PA), req(PA, MESSAGES)),
      (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.cause, "model_reconnect_required");
        assert.equal(e.retryable, false);
        return true;
      },
    );

    state.modelCredentialRefresh = () =>
      chatgpt(baseUrl, "tok-x", { version: "v2" });
    await assert.rejects(
      collect(provider(PA), req(PA, MESSAGES)),
      (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(
          e.retryable,
          true,
          "a changed selection retries the turn on the new binding",
        );
        return true;
      },
    );
  });
});

test("reconnect-required and disabled bindings fail before any request", async () => {
  await withBackend([textOnly], async (baseUrl, seen) => {
    const { state, provider } = await setup();
    const p = provider(PA);
    const probe = () => {
      assert.ok(p.probe);
      return p.probe();
    };
    state.setModelBinding(
      PA,
      chatgpt(
        baseUrl,
        "",
        {},
        {
          api_key: undefined,
          credential_available: false,
          credential_state: "reconnect_required",
        },
      ),
    );
    await assert.rejects(probe(), (e: unknown) => {
      assert.ok(e instanceof ModelError);
      assert.equal(e.cause, "model_reconnect_required");
      assert.equal(e.unavailable, true);
      return true;
    });
    state.setModelBinding(
      PA,
      chatgpt(
        baseUrl,
        "",
        {},
        {
          api_key: undefined,
          credential_available: false,
          credential_state: "disabled",
        },
      ),
    );
    await assert.rejects(probe(), (e: unknown) => {
      assert.ok(e instanceof ModelError);
      assert.equal(e.cause, "no_model_connection");
      return true;
    });
    assert.equal(seen.length, 0);
  });
});

test("usage limits fail visibly with the reset time; an ordinary 429 stays retryable", async () => {
  const limit: Reply = (_s, res) => {
    res.writeHead(429, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        error: {
          type: "usage_limit_reached",
          message: "limit",
          plan_type: "plus",
          resets_at: 1_900_000_000,
        },
      }),
    );
  };
  const busy: Reply = (_s, res) => {
    res.writeHead(429, {
      "content-type": "application/json",
      "retry-after": "2",
    });
    res.end('{"error":{"type":"rate_limit_exceeded"}}');
  };
  await withBackend([limit, busy], async (baseUrl) => {
    const { state, provider, req } = await setup();
    state.setModelBinding(PA, chatgpt(baseUrl, "tok-1"));
    await assert.rejects(
      collect(provider(PA), req(PA, MESSAGES)),
      (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, false);
        assert.equal(e.cause, "model_usage_limit");
        assert.match(e.message, /resets at 2030-03-17T/);
        return true;
      },
    );
    await assert.rejects(
      collect(provider(PA), req(PA, MESSAGES)),
      (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.equal(e.retryAfterMs, 2000);
        assert.equal(e.cause, undefined);
        return true;
      },
    );
  });
});

test("a caller abort mid-stream stops the call without a refresh or a retry", async () => {
  let release: () => void = () => {};
  const hanging: Reply = (_s, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(
      sse([
        {
          type: "response.output_text.delta",
          item_id: "m",
          output_index: 0,
          delta: "partial",
        },
      ]),
    );
    release();
  };
  await withBackend([hanging], async (baseUrl, seen) => {
    const { state, provider, req } = await setup();
    state.setModelBinding(PA, chatgpt(baseUrl, "tok-1"));
    const ctl = new AbortController();
    const started = new Promise<void>((r) => (release = r));
    const run = collect(provider(PA), req(PA, MESSAGES, ctl.signal));
    await started;
    ctl.abort(new Error("caller cancelled"));
    await assert.rejects(run);
    assert.equal(seen.length, 1);
    assert.equal(state.modelCredentialRefreshCalls.length, 0);
  });
});
