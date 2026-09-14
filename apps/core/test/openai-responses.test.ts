import assert from "node:assert/strict";
import http from "node:http";
import { test } from "node:test";
import {
  type ChatMessage,
  ModelError,
  type ModelEvent,
} from "../src/provider.ts";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";

/**
 * The real OpenAIResponsesProvider against a scripted Responses SSE
 * endpoint — no mocks inside the adapter. Each case arms the handler
 * with a status/headers/body script; `withServer` wires a throwaway
 * loopback server per test.
 */

type Captured = { req: http.IncomingMessage; body: string };
type Script = (
  req: http.IncomingMessage,
  res: http.ServerResponse,
  body: string,
) => void;

async function withServer(
  script: Script,
  fn: (baseUrl: string, seen: Captured[]) => Promise<void>,
): Promise<void> {
  const seen: Captured[] = [];
  const srv = http.createServer((req, res) => {
    let body = "";
    req.on("data", (d) => (body += d));
    req.on("end", () => {
      seen.push({ req, body });
      script(req, res, body);
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

function provider(
  baseUrl: string,
  over: Partial<ConstructorParameters<typeof OpenAIResponsesProvider>[0]> = {},
): OpenAIResponsesProvider {
  return new OpenAIResponsesProvider({
    baseUrl,
    apiKey: "test-key",
    model: "test-model",
    timeoutMs: 5_000,
    ...over,
  });
}

const TOOLS = [
  {
    name: "journal.note",
    description: "save a note",
    parameters: {
      type: "object",
      properties: { text: { type: "string" } },
    },
  },
];

function req(
  messages: ChatMessage[] = [{ role: "user", content: "hi" }],
  tools = TOOLS,
  signal?: AbortSignal,
) {
  return { personaId: "p", turnId: "t", round: 0, messages, tools, signal };
}

async function collect(p: OpenAIResponsesProvider, r = req()) {
  const out: ModelEvent[] = [];
  for await (const ev of p.stream(r)) out.push(ev);
  return out;
}

const sse =
  (events: (string | [string, string])[]) => (res: http.ServerResponse) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end(
      events
        .map((e) =>
          typeof e === "string"
            ? `data: ${e}\n\n`
            : `event: ${e[0]}\ndata: ${e[1]}\n\n`,
        )
        .join(""),
    );
  };

const ev = (obj: unknown) => JSON.stringify(obj);
const textDelta = (s: string) =>
  ev({
    type: "response.output_text.delta",
    item_id: "msg_1",
    output_index: 0,
    content_index: 0,
    delta: s,
  });
const completed = (
  output: unknown[] = [],
  usage = { input_tokens: 5, output_tokens: 3, total_tokens: 8 },
) =>
  ev({
    type: "response.completed",
    response: { status: "completed", usage, output },
  });

test("clean stream: incremental text, usage, exactly one done", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({ type: "response.created", response: { status: "in_progress" } }),
        textDelta("hel"),
        textDelta("lo"),
        completed(),
      ])(res),
    async (base) => {
      const evs = await collect(provider(base));
      assert.equal(
        evs
          .filter((e) => e.type === "text")
          .map((e) => e.delta)
          .join(""),
        "hello",
      );
      assert.equal(evs.filter((e) => e.type === "done").length, 1);
      assert.deepEqual(evs.at(-1), {
        type: "done",
        usage: { input_tokens: 5, output_tokens: 3, total_tokens: 8 },
      });
    },
  );
});

test("request shape: instructions, input items, function tools, store off", async () => {
  await withServer(
    (_req, res) => sse([completed()])(res),
    async (base, seen) => {
      await collect(
        provider(base, { maxOutputTokens: 1234 }),
        req(
          [
            { role: "system", content: "be terse" },
            { role: "user", content: "hi" },
          ],
          TOOLS,
        ),
      );
      const body = JSON.parse(seen[0]!.body) as Record<string, unknown>;
      assert.equal(body.model, "test-model");
      assert.equal(body.stream, true);
      assert.equal(body.store, false);
      assert.equal(body.max_output_tokens, 1234);
      assert.equal(body.instructions, "be terse");
      assert.deepEqual(body.input, [
        {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "hi" }],
        },
      ]);
      const tools = body.tools as Record<string, unknown>[];
      assert.equal(tools.length, 1);
      assert.equal(tools[0]!.type, "function");
      // Canonical "journal.note" → wire-safe name; parameters carry the
      // route envelope.
      assert.equal(tools[0]!.name, "journal_note");
      const params = tools[0]!.parameters as Record<string, unknown>;
      assert.deepEqual(params.required, ["route", "input"]);
    },
  );
});

test("tool call: item events assemble one call with id, wire name, route", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "response.output_item.added",
          output_index: 0,
          item: {
            type: "function_call",
            id: "fc_1",
            call_id: "call_abc",
            name: "journal_note",
            arguments: "",
          },
        }),
        ev({
          type: "response.function_call_arguments.delta",
          item_id: "fc_1",
          output_index: 0,
          delta: '{"route":"elevated","input":{"text":"a"',
        }),
        ev({
          type: "response.function_call_arguments.delta",
          item_id: "fc_1",
          output_index: 0,
          delta: 'nd b"}}',
        }),
        ev({
          type: "response.output_item.done",
          output_index: 0,
          item: {
            type: "function_call",
            id: "fc_1",
            call_id: "call_abc",
            name: "journal_note",
            arguments: '{"route":"elevated","input":{"text":"a nd b"}}',
          },
        }),
        completed([
          {
            type: "function_call",
            id: "fc_1",
            call_id: "call_abc",
            name: "journal_note",
            arguments: '{"route":"elevated","input":{"text":"a nd b"}}',
          },
        ]),
      ])(res),
    async (base) => {
      const evs = await collect(provider(base));
      const calls = evs.filter((e) => e.type === "tool_call");
      assert.equal(calls.length, 1);
      assert.deepEqual(calls[0]!.call, {
        id: "call_abc",
        name: "journal.note",
        route: "elevated",
        arguments: { text: "a nd b" },
      });
      assert.equal(evs.at(-1)!.type, "done");
    },
  );
});

test("tool results feed back as function_call_output keyed by call_id", async () => {
  await withServer(
    (_req, res) => sse([completed()])(res),
    async (base, seen) => {
      await collect(
        provider(base),
        req(
          [
            { role: "user", content: "note this" },
            {
              role: "assistant",
              content: "saving",
              toolCalls: [
                {
                  id: "call_abc",
                  name: "journal.note",
                  route: "elevated",
                  arguments: { text: "x" },
                },
              ],
            },
            { role: "tool", toolCallId: "call_abc", content: '{"ok":true}' },
          ],
          TOOLS,
        ),
      );
      const input = (JSON.parse(seen[0]!.body) as { input: unknown[] }).input;
      assert.deepEqual(input, [
        {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: "note this" }],
        },
        {
          // Easy input-message form: assistant replay needs no item id
          // (the output-message form requires one the journal never kept).
          type: "message",
          role: "assistant",
          content: "saving",
        },
        {
          type: "function_call",
          call_id: "call_abc",
          name: "journal_note",
          arguments: '{"route":"elevated","input":{"text":"x"}}',
          status: "completed",
        },
        {
          type: "function_call_output",
          call_id: "call_abc",
          output: '{"ok":true}',
        },
      ]);
    },
  );
});

test("calls recoverable from the terminal response when item events are absent", async () => {
  await withServer(
    (_req, res) =>
      sse([
        completed([
          {
            type: "function_call",
            call_id: "call_z",
            name: "journal_note",
            arguments: '{"route":"normal","input":{"text":"q"}}',
          },
        ]),
      ])(res),
    async (base) => {
      const evs = await collect(provider(base));
      const calls = evs.filter((e) => e.type === "tool_call");
      assert.equal(calls.length, 1);
      assert.deepEqual(calls[0]!.call, {
        id: "call_z",
        name: "journal.note",
        route: "normal",
        arguments: { text: "q" },
      });
    },
  );
});

test("a call without a route envelope is malformed, not silently normal", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "response.output_item.added",
          output_index: 0,
          item: {
            type: "function_call",
            id: "fc_1",
            call_id: "call_1",
            name: "journal_note",
          },
        }),
        ev({
          type: "response.function_call_arguments.done",
          item_id: "fc_1",
          output_index: 0,
          arguments: '{"input":{"text":"x"}}',
        }),
        completed(),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) =>
          e instanceof ModelError && e.retryable && /malformed/.test(e.message),
      );
    },
  );
});

test("http errors: 401 permanent, 429 keeps Retry-After, 5xx retryable", async () => {
  const cases: [number, Record<string, string>, string, boolean, number?][] = [
    [401, {}, "bad key", false],
    [429, { "retry-after": "7" }, "slow down", true, 7000],
    [503, {}, "overloaded", true],
    [413, {}, "request too large", false],
  ];
  for (const [status, headers, body, retryable, retryAfterMs] of cases) {
    await withServer(
      (_req, res) => {
        res.writeHead(status, headers);
        res.end(body);
      },
      async (base) => {
        await assert.rejects(collect(provider(base)), (e: unknown) => {
          assert.ok(e instanceof ModelError, `status ${status}`);
          assert.equal(e.retryable, retryable, `status ${status}`);
          if (retryAfterMs !== undefined) {
            assert.equal(e.retryAfterMs, retryAfterMs);
          }
          if (status === 413) assert.equal(e.refusal, "context_length");
          return true;
        });
      },
    );
  }
});

test("response.incomplete max_output_tokens completes truncated like finish_reason=length", async () => {
  await withServer(
    (_req, res) =>
      sse([
        textDelta("partial"),
        ev({
          type: "response.incomplete",
          response: {
            status: "incomplete",
            incomplete_details: { reason: "max_output_tokens" },
            usage: { input_tokens: 9, output_tokens: 4 },
          },
        }),
      ])(res),
    async (base) => {
      // The output cap is not an input-context refusal: the turn gets the
      // truncated text, recorded in usage like the other wires' length
      // finish — never a halved-context retry.
      const evs = await collect(provider(base));
      assert.deepEqual(evs[0], { type: "text", delta: "partial" });
      const done = evs.at(-1);
      assert.equal(done?.type, "done");
      assert.equal(
        (done as { usage: { finish_reason?: string } }).usage.finish_reason,
        "max_output_tokens",
      );
      assert.equal(
        (done as { usage: { output_tokens?: number } }).usage.output_tokens,
        4,
      );
    },
  );
});

test("response.incomplete content_filter is a deterministic refusal", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "response.incomplete",
          response: {
            status: "incomplete",
            incomplete_details: { reason: "content_filter" },
          },
        }),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) => e instanceof ModelError && !e.retryable,
      );
    },
  );
});

test("response.incomplete for other reasons retries", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "response.incomplete",
          response: {
            status: "incomplete",
            incomplete_details: { reason: "server_error" },
          },
        }),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) => e instanceof ModelError && e.retryable,
      );
    },
  );
});

test("response.failed and mid-stream error events fail honestly", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "response.failed",
          response: {
            status: "failed",
            error: { code: "server_error", message: "blew up" },
          },
        }),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) =>
          e instanceof ModelError && e.retryable && /blew up/.test(e.message),
      );
    },
  );
  await withServer(
    (_req, res) =>
      sse([
        textDelta("partial"),
        ev({ type: "error", code: "invalid_request", message: "bad stream" }),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) => e instanceof ModelError && !e.retryable,
      );
    },
  );
});

test("a stream that ends without response.completed is not a reply", async () => {
  await withServer(
    (_req, res) => sse([textDelta("cut off")])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) =>
          e instanceof ModelError &&
          e.retryable &&
          /incomplete/.test(e.message),
      );
    },
  );
});

test("malformed SSE data is a transient failure", async () => {
  await withServer(
    (_req, res) => sse(["{not json"])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) => e instanceof ModelError && e.retryable,
      );
    },
  );
});

test("caller cancellation aborts the request mid-stream", async () => {
  const ac = new AbortController();
  let requestEnded = false;
  await withServer(
    (_req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      res.write(`data: ${textDelta("first")}\n\n`);
      res.on("close", () => {
        requestEnded = true;
      });
      // Never finish — the abort must close it.
    },
    async (base) => {
      const p = provider(base);
      const iter = p
        .stream(req(undefined, TOOLS, ac.signal))
        [Symbol.asyncIterator]();
      const first = await iter.next();
      assert.deepEqual(first.value, { type: "text", delta: "first" });
      ac.abort(new Error("stop"));
      await assert.rejects(iter.next());
      await new Promise((r) => setTimeout(r, 50));
      assert.equal(requestEnded, true, "server saw the request close");
    },
  );
});

test("per-connection extra headers reach only this endpoint", async () => {
  await withServer(
    (_req, res) => sse([completed()])(res),
    async (base, seen) => {
      await collect(
        provider(base, { headers: { "X-Gateway-Session": "s-1" } }),
      );
      assert.equal(seen[0]!.req.headers["x-gateway-session"], "s-1");
      assert.equal(seen[0]!.req.headers.authorization, "Bearer test-key");
      assert.equal(seen[0]!.req.url, "/responses");
    },
  );
});

test("unparseable tool arguments fail the call", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "response.output_item.added",
          output_index: 0,
          item: {
            type: "function_call",
            id: "fc_1",
            call_id: "call_1",
            name: "journal_note",
          },
        }),
        ev({
          type: "response.function_call_arguments.done",
          item_id: "fc_1",
          output_index: 0,
          arguments: "{broken",
        }),
        completed(),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) =>
          e instanceof ModelError &&
          /unparseable tool arguments/.test(e.message),
      );
    },
  );
});

test("a reserved extra header fails the call, never the credential", async () => {
  await assert.rejects(
    collect(
      provider("http://127.0.0.1:1", {
        headers: { "Content-Type": "text/plain" },
      }),
    ),
    (e: unknown) =>
      e instanceof ModelError && !e.retryable && /reserved/.test(e.message),
  );
});
