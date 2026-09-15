import assert from "node:assert/strict";
import http from "node:http";
import { test } from "node:test";
import {
  type ChatMessage,
  ModelError,
  type ModelEvent,
} from "../src/provider.ts";
import { AnthropicProvider } from "../src/providers/anthropic.ts";

/**
 * The real AnthropicProvider against a scripted Messages SSE endpoint —
 * no mocks inside the adapter. Each case arms the handler with a
 * status/headers/body script; `withServer` wires a throwaway loopback
 * server per test.
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
  over: Partial<ConstructorParameters<typeof AnthropicProvider>[0]> = {},
): AnthropicProvider {
  return new AnthropicProvider({
    baseUrl,
    apiKey: "test-key",
    model: "claude-test",
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

async function collect(p: AnthropicProvider, r = req()) {
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
    type: "content_block_delta",
    index: 0,
    delta: { type: "text_delta", text: s },
  });
const stop = (reason = "end_turn") =>
  ev({
    type: "message_delta",
    delta: { stop_reason: reason },
    usage: { output_tokens: 4 },
  });
const messageStop = ev({ type: "message_stop" });
const messageStart = ev({
  type: "message_start",
  message: { usage: { input_tokens: 9, output_tokens: 1 } },
});

test("clean stream: headers, incremental text, usage, exactly one done", async () => {
  await withServer(
    (_req, res) =>
      sse([
        messageStart,
        textDelta("hel"),
        textDelta("lo"),
        stop(),
        messageStop,
      ])(res),
    async (base, seen) => {
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
        usage: { input_tokens: 9, output_tokens: 4, finish_reason: "end_turn" },
      });
      const r = seen[0]!.req;
      assert.equal(r.url, "/v1/messages");
      assert.equal(r.headers["x-api-key"], "test-key");
      assert.equal(r.headers["anthropic-version"], "2023-06-01");
      assert.equal(r.headers.authorization, undefined);
    },
  );
});

test("request shape: system field, messages, tool input_schema, stream", async () => {
  await withServer(
    (_req, res) => sse([messageStop])(res),
    async (base, seen) => {
      await collect(
        provider(base, { maxTokens: 777 }),
        req([
          { role: "system", content: "be terse" },
          { role: "user", content: "hi" },
        ]),
      );
      const body = JSON.parse(seen[0]!.body) as Record<string, unknown>;
      assert.equal(body.model, "claude-test");
      assert.equal(body.stream, true);
      assert.equal(body.max_tokens, 777);
      assert.equal(body.system, "be terse");
      assert.deepEqual(body.messages, [
        { role: "user", content: [{ type: "text", text: "hi" }] },
      ]);
      const tools = body.tools as Record<string, unknown>[];
      assert.equal(tools[0]!.name, "journal_note");
      const schema = tools[0]!.input_schema as Record<string, unknown>;
      assert.deepEqual(schema.required, ["route", "input"]);
      // No "messages" role map leakage, no top-level user/assistant mixup.
      assert.equal((body as Record<string, unknown>).instructions, undefined);
    },
  );
});

test("tool_use blocks assemble one call preserving id, wire name, route", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "content_block_start",
          index: 0,
          content_block: { type: "text", text: "" },
        }),
        textDelta("noting"),
        ev({ type: "content_block_stop", index: 0 }),
        ev({
          type: "content_block_start",
          index: 1,
          content_block: {
            type: "tool_use",
            id: "toolu_01X",
            name: "journal_note",
            input: {},
          },
        }),
        ev({
          type: "content_block_delta",
          index: 1,
          delta: {
            type: "input_json_delta",
            partial_json: '{"route":"elevated","input":{"text":"a',
          },
        }),
        ev({
          type: "content_block_delta",
          index: 1,
          delta: { type: "input_json_delta", partial_json: 'nd b"}}' },
        }),
        ev({ type: "content_block_stop", index: 1 }),
        stop("tool_use"),
        messageStop,
      ])(res),
    async (base) => {
      const evs = await collect(provider(base));
      const calls = evs.filter((e) => e.type === "tool_call");
      assert.equal(calls.length, 1);
      assert.deepEqual(calls[0]!.call, {
        id: "toolu_01X",
        name: "journal.note",
        route: "elevated",
        arguments: { text: "and b" },
      });
      const done = evs.at(-1)!;
      assert.equal(done.type, "done");
      assert.equal(done.usage.finish_reason, "tool_use");
    },
  );
});

test("tool results feed back as tool_result blocks in one user message", async () => {
  await withServer(
    (_req, res) => sse([messageStop])(res),
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
                  id: "toolu_01X",
                  name: "journal.note",
                  route: "elevated",
                  arguments: { text: "x" },
                },
                {
                  id: "toolu_02Y",
                  name: "journal.note",
                  route: "normal",
                  arguments: { text: "y" },
                },
              ],
            },
            { role: "tool", toolCallId: "toolu_01X", content: "saved x" },
            { role: "tool", toolCallId: "toolu_02Y", content: "saved y" },
          ],
          TOOLS,
        ),
      );
      const messages = (JSON.parse(seen[0]!.body) as { messages: unknown[] })
        .messages;
      assert.deepEqual(messages, [
        { role: "user", content: [{ type: "text", text: "note this" }] },
        {
          role: "assistant",
          content: [
            { type: "text", text: "saving" },
            {
              type: "tool_use",
              id: "toolu_01X",
              name: "journal_note",
              input: { route: "elevated", input: { text: "x" } },
            },
            {
              type: "tool_use",
              id: "toolu_02Y",
              name: "journal_note",
              input: { route: "normal", input: { text: "y" } },
            },
          ],
        },
        {
          role: "user",
          content: [
            {
              type: "tool_result",
              tool_use_id: "toolu_01X",
              content: "saved x",
            },
            {
              type: "tool_result",
              tool_use_id: "toolu_02Y",
              content: "saved y",
            },
          ],
        },
      ]);
    },
  );
});

test("http errors: 401 permanent, 429 keeps Retry-After, 5xx retryable", async () => {
  const cases: [number, Record<string, string>, string, boolean, number?][] = [
    [
      401,
      {},
      '{"error":{"type":"authentication_error","message":"bad key"}}',
      false,
    ],
    [
      429,
      { "retry-after": "7" },
      '{"error":{"type":"rate_limit_error","message":"slow down"}}',
      true,
      7000,
    ],
    [
      529,
      {},
      '{"error":{"type":"overloaded_error","message":"overloaded"}}',
      true,
    ],
    [
      413,
      {},
      '{"error":{"type":"request_too_large","message":"too big"}}',
      false,
    ],
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

test("mid-stream error events classify by type", async () => {
  await withServer(
    (_req, res) =>
      sse([
        textDelta("partial"),
        ev({
          type: "error",
          error: { type: "overloaded_error", message: "overloaded" },
        }),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) => e instanceof ModelError && e.retryable,
      );
    },
  );
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "error",
          error: { type: "invalid_request_error", message: "bad" },
        }),
      ])(res),
    async (base) => {
      await assert.rejects(
        collect(provider(base)),
        (e: unknown) => e instanceof ModelError && !e.retryable,
      );
    },
  );
  // api_error is Anthropic's generic 500-class condition — transient.
  await withServer(
    (_req, res) =>
      sse([
        textDelta("partial"),
        ev({
          type: "error",
          error: { type: "api_error", message: "internal error" },
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

test("an empty user message emits no text block (Anthropic rejects them)", async () => {
  await withServer(
    (_req, res) => sse([messageStop])(res),
    async (base, seen) => {
      await collect(
        provider(base),
        req([
          { role: "user", content: "hi" },
          { role: "user", content: "" },
          { role: "tool", toolCallId: "t1", content: "result" },
        ]),
      );
      const body = JSON.parse(seen[0]!.body) as {
        messages: {
          role: string;
          content: { type: string; text?: string }[];
        }[];
      };
      // The empty user message contributes no block; the tool_result still
      // coalesces into the surviving user turn.
      assert.equal(body.messages.length, 1);
      assert.equal(body.messages[0]!.role, "user");
      assert.deepEqual(
        body.messages[0]!.content.map((b) => b.type),
        ["text", "tool_result"],
      );
      assert.equal(body.messages[0]!.content[0]!.text, "hi");
    },
  );
});

test("a reserved extra header fails the call, never the credential", async () => {
  await assert.rejects(
    collect(
      provider("http://127.0.0.1:1", {
        headers: { "X-Api-Key": "spoof" },
      }),
    ),
    (e: unknown) =>
      e instanceof ModelError && !e.retryable && /reserved/.test(e.message),
  );
});

test("a stream that ends without message_stop is not a reply", async () => {
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

test("a call without a route envelope is malformed, not silently normal", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "content_block_start",
          index: 0,
          content_block: {
            type: "tool_use",
            id: "toolu_1",
            name: "journal_note",
          },
        }),
        ev({
          type: "content_block_delta",
          index: 0,
          delta: {
            type: "input_json_delta",
            partial_json: '{"input":{"text":"x"}}',
          },
        }),
        ev({ type: "content_block_stop", index: 0 }),
        messageStop,
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
    (_req, res) => sse([messageStop])(res),
    async (base, seen) => {
      await collect(
        provider(base, { headers: { "X-Gateway-Session": "s-1" } }),
      );
      assert.equal(seen[0]!.req.headers["x-gateway-session"], "s-1");
      assert.equal(seen[0]!.req.headers["x-api-key"], "test-key");
    },
  );
});

test("a configured session header carries the persona's stable identity", async () => {
  await withServer(
    (_req, res) => sse([messageStop])(res),
    async (base, seen) => {
      await collect(
        provider(base, {
          sessionHeader: "x-opencode-session",
          headers: { "X-OPENCODE-SESSION": "stale" },
        }),
      );
      // The live identity wins as a single value — no "stale, p" combine.
      assert.equal(seen[0]!.req.headers["x-opencode-session"], "p");
    },
  );
});

test("the honest Sumi User-Agent is the default and stays overridable", async () => {
  await withServer(
    (_req, res) => sse([messageStop])(res),
    async (base, seen) => {
      await collect(provider(base));
      assert.equal(seen[0]!.req.headers["user-agent"], "sumi-secretary/alpha");
      await collect(
        provider(base, { headers: { "USER-AGENT": "operator-agent/1" } }),
      );
      assert.equal(seen[1]!.req.headers["user-agent"], "operator-agent/1");
    },
  );
});

test("unparseable tool arguments fail the call", async () => {
  await withServer(
    (_req, res) =>
      sse([
        ev({
          type: "content_block_start",
          index: 0,
          content_block: {
            type: "tool_use",
            id: "toolu_1",
            name: "journal_note",
          },
        }),
        ev({
          type: "content_block_delta",
          index: 0,
          delta: { type: "input_json_delta", partial_json: "{broken" },
        }),
        ev({ type: "content_block_stop", index: 0 }),
        messageStop,
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

test("a replayed call keeps a valid wire name after the tool leaves the advertised set", async () => {
  await withServer(
    (_req, res) => sse([messageStop])(res),
    async (base, seen) => {
      // journal.note ran in an earlier generation; this consultation no
      // longer advertises it (tools: []). The replayed canonical name
      // must still become a wire-legal name — Anthropic rejects dotted
      // names outright (400) — and the tool_use_id linkage survives.
      await collect(
        provider(base),
        req(
          [
            {
              role: "assistant",
              content: "",
              toolCalls: [
                {
                  id: "toolu_01X",
                  name: "journal.note",
                  route: "normal",
                  arguments: { text: "x" },
                },
              ],
            },
            { role: "tool", toolCallId: "toolu_01X", content: "saved x" },
          ],
          [],
        ),
      );
      const messages = (JSON.parse(seen[0]!.body) as { messages: unknown[] })
        .messages as { role: string; content: Record<string, unknown>[] }[];
      const toolUse = messages
        .find((m) => m.role === "assistant")!
        .content.find((b) => b.type === "tool_use")!;
      assert.equal(toolUse.name, "journal_note");
      assert.equal(toolUse.id, "toolu_01X");
      const toolResult = messages
        .find((m) => m.role === "user")!
        .content.find((b) => b.type === "tool_result")!;
      assert.equal(toolResult.tool_use_id, "toolu_01X");
    },
  );
});

test("model_context_window_exceeded completes truncated with faithful finish metadata", async () => {
  await withServer(
    (_req, res) =>
      sse([
        textDelta("cut by window"),
        stop("model_context_window_exceeded"),
        messageStop,
      ])(res),
    async (base) => {
      // Anthropic documents this stop_reason as a filled context window —
      // "treat the response as truncated". It is not an input-capacity
      // refusal and does not engage the smaller-view retry: the reply is
      // the truncated text with the true reason recorded in usage.
      const evs = await collect(provider(base));
      assert.deepEqual(evs[0], { type: "text", delta: "cut by window" });
      const done = evs.at(-1)!;
      assert.equal(done.type, "done");
      assert.equal(
        (done as { usage: { finish_reason?: string } }).usage.finish_reason,
        "model_context_window_exceeded",
      );
    },
  );
});
