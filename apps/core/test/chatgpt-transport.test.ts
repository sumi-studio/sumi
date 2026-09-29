import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ModelError,
  type ModelEvent,
  type ModelRequest,
} from "../src/provider.ts";
import {
  CHATGPT_MAX_REQUEST_BYTES,
  CHATGPT_REJECTED_HEADER,
  continuationScope,
  MAX_CONTINUATION_BYTES,
  utf8Length,
} from "../src/providers/chatgpt-codex.ts";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";
import { HttpStateClient } from "../src/state-client.ts";

const request: ModelRequest = {
  personaId: "p",
  turnId: "t",
  round: 0,
  messages: [{ role: "user", content: "synthetic" }],
  tools: [],
};
const completed =
  'data: {"type":"response.completed","response":{"status":"completed"}}\n\n';
/** Set by the Go route on every response it authors. */
const MARK = { "X-Sumi-Model-Transport": "1" };
async function collect(p: OpenAIResponsesProvider): Promise<ModelEvent[]> {
  const out: ModelEvent[] = [];
  for await (const e of p.stream(request)) out.push(e);
  return out;
}

test("subscription transport uses only the configured state origin, Core auth and model deadline", async () => {
  const signal = new AbortController().signal;
  const client = new HttpStateClient(
    "https://state.test",
    "service-secret",
    async (url, init) => {
      assert.equal(
        url,
        "https://state.test/internal/core/personas/p/model/chatgpt/responses",
      );
      assert.equal(init?.signal, signal);
      assert.equal(init?.redirect, "manual");
      assert.deepEqual(init?.headers, {
        Authorization: "Bearer service-secret",
        "Content-Type": "application/json",
      });
      assert.deepEqual(JSON.parse(init?.body ?? ""), {
        connection_id: "c",
        connection_version: "v",
        request: { model: "m" },
        rejected_token_sha256: "a".repeat(64),
        timeout_ms: 250_000,
      });
      return new Response(completed, {
        headers: { "Content-Type": "text/event-stream", ...MARK },
      });
    },
    1,
  );
  const res = await client.chatGPTResponses(
    "p",
    "c",
    "v",
    '{"model":"m"}',
    signal,
    "a".repeat(64),
    250_000,
  );
  assert.equal(await res.text(), completed);
});

test("transport admission errors are fixed messages and service 401 does not trigger token refresh", async () => {
  for (const code of [
    "binding_changed",
    "credential_unavailable",
    "model_reconnect_required",
    "model_connection_disabled",
    "transport_ambiguous",
  ]) {
    const state = new HttpStateClient(
      "https://state.test",
      "token",
      async () =>
        new Response("secret-body", {
          status: 409,
          headers: { "X-Sumi-Model-Error": code, ...MARK },
        }),
    );
    await assert.rejects(
      state.chatGPTResponses("p", "c", "v", "{}", new AbortController().signal),
      (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(
          e.retryable,
          ["binding_changed", "credential_unavailable"].includes(code),
        );
        assert.ok(!e.message.includes("secret-body"));
        return true;
      },
    );
  }
  let calls = 0;
  const state = new HttpStateClient("https://state.test", "token", async () => {
    calls++;
    return new Response("private", { status: 401, headers: MARK });
  });
  const p = new OpenAIResponsesProvider({
    baseUrl: "unused",
    apiKey: "",
    model: "gpt-6-astra",
    chatgpt: {
      accountId: "account",
      send: (body, signal, rejected) =>
        state.chatGPTResponses("p", "c", "v", body, signal, rejected),
    },
  });
  await assert.rejects(
    collect(p),
    (e: unknown) =>
      e instanceof ModelError &&
      !e.retryable &&
      e.message === "Core service authorization failed",
  );
  assert.equal(calls, 1);
});

test("subscription transport never automatically retries ambiguous network or truncated streams", async () => {
  for (const mode of ["network", "truncated", "stream-error", "timeout"]) {
    let calls = 0;
    const p = new OpenAIResponsesProvider({
      baseUrl: "unused",
      apiKey: "",
      model: "gpt-6-astra",
      timeoutMs: 10,
      chatgpt: {
        accountId: "account",
        send: async (_body, signal) => {
          calls++;
          if (mode === "network") throw new TypeError("synthetic disconnect");
          if (mode === "timeout")
            return await new Promise<Response>((_resolve, reject) => {
              const timer = setTimeout(
                () => reject(new Error("fixture hung")),
                1000,
              );
              signal.addEventListener(
                "abort",
                () => {
                  clearTimeout(timer);
                  reject(signal.reason);
                },
                { once: true },
              );
            });
          if (mode === "stream-error")
            return new Response(
              new ReadableStream({
                start(c) {
                  c.error(new Error("synthetic disconnect"));
                },
              }),
            );
          return new Response(
            'data: {"type":"response.output_text.delta","delta":"partial"}\n\n',
          );
        },
      },
    });
    await assert.rejects(
      collect(p),
      (e: unknown) => e instanceof ModelError && !e.retryable,
    );
    assert.equal(calls, 1);
  }
});

test("403 is a terminal refusal, and only an explicit upstream 401 permits one digest resend", async () => {
  for (const status of [403, 401]) {
    const digests: (string | undefined)[] = [];
    const p = new OpenAIResponsesProvider({
      baseUrl: "unused",
      apiKey: "",
      model: "gpt-6-astra",
      chatgpt: {
        accountId: "account",
        send: async (_body, _signal, digest) => {
          digests.push(digest);
          return new Response('{"error":{"code":"token_expired"}}', {
            status,
            headers:
              status === 401
                ? { [CHATGPT_REJECTED_HEADER]: "b".repeat(64) }
                : {},
          });
        },
      },
    });
    await assert.rejects(
      collect(p),
      (e: unknown) => e instanceof ModelError && !e.retryable,
    );
    assert.deepEqual(
      digests,
      status === 401 ? [undefined, "b".repeat(64)] : [undefined],
    );
  }
});

test("subscription stream failures never copy raw upstream error text into recorded errors", async () => {
  for (const event of [
    {
      type: "response.failed",
      response: {
        error: { message: "private-token", code: "permission_denied" },
      },
    },
    { type: "error", message: "private-token", code: "permission_denied" },
    {
      type: "response.incomplete",
      response: { incomplete_details: { reason: "private-token" } },
    },
  ]) {
    const p = new OpenAIResponsesProvider({
      baseUrl: "unused",
      apiKey: "",
      model: "gpt-6-astra",
      chatgpt: {
        accountId: "account",
        send: async () => new Response(`data: ${JSON.stringify(event)}\n\n`),
      },
    });
    await assert.rejects(
      collect(p),
      (e: unknown) =>
        e instanceof ModelError &&
        !e.retryable &&
        !e.message.includes("private-token"),
    );
  }
});

function transportProvider(
  fetchImpl: ConstructorParameters<typeof HttpStateClient>[2],
  model = "gpt-6-astra",
  timeoutMs?: number,
): OpenAIResponsesProvider {
  const state = new HttpStateClient("https://state.test", "token", fetchImpl);
  return new OpenAIResponsesProvider({
    baseUrl: "unused",
    apiKey: "",
    model,
    timeoutMs,
    chatgpt: {
      accountId: "account",
      send: (body, signal, rejected, remaining) =>
        state.chatGPTResponses(
          "p",
          "c",
          "v",
          body,
          signal,
          rejected,
          remaining,
        ),
    },
  });
}

async function failure(p: OpenAIResponsesProvider, r = request) {
  try {
    for await (const _ of p.stream(r)) {
      /* drain */
    }
  } catch (e) {
    assert.ok(e instanceof ModelError, String(e));
    return e;
  }
  assert.fail("stream did not fail");
}

test("an unmarked intermediary response is ambiguous and never replayed; marked upstream errors keep their classification", async () => {
  // A tunnel/VPC gateway can answer after the API already dispatched.
  for (const status of [200, 404, 502, 503, 504, 530]) {
    let calls = 0;
    let cancelled = false;
    const e = await failure(
      transportProvider(async () => {
        calls++;
        return new Response(
          new ReadableStream({
            start(c) {
              c.enqueue(
                new TextEncoder().encode("<html>private gateway</html>"),
              );
            },
            cancel() {
              cancelled = true;
            },
          }),
          { status, headers: { "Content-Type": "text/html" } },
        );
      }),
    );
    assert.equal(e.retryable, false, `HTTP ${status}`);
    assert.equal(e.unavailable, undefined);
    assert.ok(!e.message.includes("private"));
    assert.match(e.message, /did not come from the Sumi API/);
    assert.equal(calls, 1);
    assert.ok(cancelled, "unmarked body was not released");
  }
  // The API's own forwarded upstream statuses are authored responses.
  const upstream503 = await failure(
    transportProvider(
      async () =>
        new Response(
          '{"error":{"message":"ChatGPT request rejected (HTTP 503)","type":"server_error"}}',
          {
            status: 503,
            headers: { "Retry-After": "3", ...MARK },
          },
        ),
    ),
  );
  assert.equal(upstream503.retryable, true);
  assert.equal(upstream503.retryAfterMs, 3000);
  const quota = await failure(
    transportProvider(
      async () =>
        new Response(
          '{"error":{"message":"ChatGPT request rejected (HTTP 429)","type":"usage_limit_reached","resets_at":1900000000}}',
          { status: 429, headers: MARK },
        ),
    ),
  );
  assert.equal(quota.retryable, false);
  assert.equal(quota.cause, "model_usage_limit");
});

test("a rejecting body cancel does not replace a classified transport error", async () => {
  for (const [status, headers, expected] of [
    [
      409,
      { "X-Sumi-Model-Error": "binding_changed", ...MARK },
      /selected connection changed/,
    ],
    [401, MARK, /Core service authorization failed/],
    [502, {}, /did not come from the Sumi API/],
  ] as const) {
    const e = await failure(
      transportProvider(async () => {
        const res = new Response("x", { status, headers });
        Object.defineProperty(res, "body", {
          value: {
            cancel: () => Promise.reject(new Error("synthetic cancel failure")),
          },
        });
        return res;
      }),
    );
    assert.match(e.message, expected);
  }
});

function continuationTurn(
  scope: string,
  rounds: number,
  bytes: number,
): ModelRequest {
  const messages: ModelRequest["messages"] = [
    { role: "user", content: "synthetic" },
  ];
  for (let r = 0; r < rounds; r++) {
    messages.push({
      role: "assistant",
      content: "",
      toolCalls: [
        {
          id: `call_${r}`,
          name: "journal.note",
          route: "normal",
          arguments: { text: "x" },
        },
      ],
      continuation: {
        scope,
        output: [
          {
            type: "reasoning",
            id: `rs_${r}`,
            encrypted_content: "A".repeat(bytes),
          },
          { type: "function_call", id: `fc_${r}`, call_id: `call_${r}` },
        ],
      },
    } as ModelRequest["messages"][number]);
    messages.push({ role: "tool", toolCallId: `call_${r}`, content: "{}" });
  }
  return { ...request, round: rounds, messages };
}

test("a continuation turn above the old 4 MiB state limit is sent whole; the transport limit drops continuation before refusing", async () => {
  const scope = await continuationScope("account", "gpt-5.5");
  const sent: number[] = [];
  const replayed: boolean[] = [];
  const p = transportProvider(async (_url, init) => {
    const body = init?.body ?? "";
    sent.push(utf8Length(body));
    replayed.push(body.includes('"encrypted_content"'));
    return new Response(completed, { headers: MARK });
  }, "gpt-5.5");
  // Five replayed rounds at 80% of the per-round continuation cap.
  const five = continuationTurn(
    scope,
    5,
    Math.floor(MAX_CONTINUATION_BYTES * 0.8),
  );
  for await (const _ of p.stream(five)) {
    /* drain */
  }
  const [first = 0] = sent;
  assert.equal(sent.length, 1);
  assert.ok(first > 4 << 20, `sent ${first} bytes`);
  assert.deepEqual(replayed, [true]);
  // Continuation that would exceed the transport limit is omitted and the
  // same turn is sent without it: one request, no refusal.
  const big = continuationTurn(scope, 6, MAX_CONTINUATION_BYTES);
  big.messages.push({
    role: "user",
    content: "B".repeat(CHATGPT_MAX_REQUEST_BYTES - 5 * MAX_CONTINUATION_BYTES),
  });
  for await (const _ of p.stream(big)) {
    /* drain */
  }
  assert.equal(sent.length, 2);
  assert.ok((sent[1] ?? Infinity) <= CHATGPT_MAX_REQUEST_BYTES);
  assert.deepEqual(replayed, [true, false]);
});

test("a request above the transport limit is a size refusal that sends nothing", async () => {
  let calls = 0;
  const p = transportProvider(async () => {
    calls++;
    return new Response(completed, { headers: MARK });
  });
  const e = await failure(p, {
    ...request,
    messages: [
      { role: "user", content: "é".repeat(CHATGPT_MAX_REQUEST_BYTES / 2) },
    ],
  });
  assert.equal(e.refusal, "context_length");
  assert.equal(e.retryable, false);
  assert.match(e.message, /transport limit; it was not sent/);
  assert.equal(calls, 0);
  // The API's own bound is classified the same way, never as an opaque
  // invalid request.
  const api = await failure(
    transportProvider(
      async () =>
        new Response('{"error":"request_too_large"}', {
          status: 413,
          headers: { "X-Sumi-Model-Error": "request_too_large", ...MARK },
        }),
    ),
  );
  assert.equal(api.refusal, "context_length");
  assert.equal(api.retryable, false);
  assert.equal(api.unavailable, true);
});

test("the transport carries the remaining configured model budget, including settings above 120 s", async () => {
  const budgets: number[] = [];
  const p = transportProvider(
    async (_url, init) => {
      budgets.push(JSON.parse(init?.body ?? "{}").timeout_ms);
      if (budgets.length === 1) {
        await new Promise((r) => setTimeout(r, 30));
        return new Response("", {
          status: 401,
          headers: { [CHATGPT_REJECTED_HEADER]: "c".repeat(64), ...MARK },
        });
      }
      return new Response(completed, { headers: MARK });
    },
    "gpt-6-astra",
    600_000,
  );
  for await (const _ of p.stream(request)) {
    /* drain */
  }
  const [initial = 0, resend = 0] = budgets;
  assert.equal(budgets.length, 2);
  assert.ok(initial > 599_000 && initial <= 600_000, String(initial));
  // The resend after refresh gets what is left, not a fresh budget.
  assert.ok(resend < initial - 20, String(budgets));
  assert.ok(resend > 120_000);
});

test("utf8Length matches the encoded size", () => {
  for (const s of [
    "",
    "ascii",
    "é",
    "日本語",
    "😀",
    "a\ud800b",
    "\udc00",
    "x😀é日",
  ]) {
    assert.equal(
      utf8Length(s),
      new TextEncoder().encode(s).length,
      JSON.stringify(s),
    );
  }
});
