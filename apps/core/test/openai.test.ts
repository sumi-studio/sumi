import assert from "node:assert/strict";
import http from "node:http";
import { test } from "node:test";
import { ModelError, type ModelEvent } from "../src/provider.ts";
import { OpenAIProvider } from "../src/providers/openai.ts";

/**
 * The real OpenAIProvider against a scripted OpenAI-compatible SSE
 * endpoint — no mocks inside the adapter. Each case arms the handler
 * with a status/headers/body script; `withServer` wires a throwaway
 * loopback server per test.
 */

type Script = (req: http.IncomingMessage, res: http.ServerResponse) => void;

async function withServer(
  script: Script,
  fn: (baseUrl: string) => Promise<void>,
): Promise<void> {
  const srv = http.createServer(script);
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const port = (srv.address() as { port: number }).port;
  try {
    await fn(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise<void>((r) => srv.close(() => r()));
  }
}

function provider(baseUrl: string, timeoutMs = 5_000): OpenAIProvider {
  return new OpenAIProvider({
    baseUrl,
    apiKey: "test-key",
    model: "test-model",
    timeoutMs,
  });
}

const REQ = {
  personaId: "p",
  turnId: "t",
  round: 0,
  messages: [{ role: "user" as const, content: "hi" }],
  tools: [],
};

async function collect(p: OpenAIProvider): Promise<ModelEvent[]> {
  const out: ModelEvent[] = [];
  for await (const ev of p.stream(REQ)) out.push(ev);
  return out;
}

const sse = (lines: string[]) => (res: http.ServerResponse) => {
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end(lines.map((l) => `data: ${l}\n\n`).join(""));
};

const chunk = (obj: unknown) => JSON.stringify(obj);
const text = (s: string) => chunk({ choices: [{ delta: { content: s } }] });
const fin = (reason: string) =>
  chunk({ choices: [{ delta: {}, finish_reason: reason }] });

test("clean stream: text, finish_reason in usage, done", async () => {
  await withServer(
    (_req, res) => sse([text("hello"), fin("stop"), "[DONE]"])(res),
    async (base) => {
      const evs = await collect(provider(base));
      assert.equal(
        evs
          .filter((e) => e.type === "text")
          .map((e) => e.delta)
          .join(""),
        "hello",
      );
      const done = evs.find((e) => e.type === "done")!;
      assert.equal(done.usage.finish_reason, "stop");
    },
  );
});

test("in-band SSE error chunk is a failure, never a reply (F2)", async () => {
  await withServer(
    (_req, res) =>
      sse([
        text("partial"),
        chunk({ error: { message: "upstream exploded", code: 503 } }),
        "[DONE]",
      ])(res),
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.match(e.message, /upstream exploded/);
        return true;
      });
    },
  );
});

test("in-band permanent error chunk is non-retryable (F2)", async () => {
  await withServer(
    (_req, res) =>
      sse([
        chunk({
          error: {
            message: "invalid model",
            code: 400,
            type: "invalid_request_error",
          },
        }),
        "[DONE]",
      ])(res),
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, false);
        return true;
      });
    },
  );
});

test("EOF without [DONE] or finish_reason is an incomplete-stream failure (F2)", async () => {
  await withServer(
    (_req, res) => sse([text("half a reply")])(res),
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.match(e.message, /incomplete/);
        return true;
      });
    },
  );
});

test("malformed SSE data is a transient failure, not a reply (F2)", async () => {
  await withServer(
    (_req, res) => sse(["{not json", "[DONE]"])(res),
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError && e.retryable);
        assert.match(e.message, /malformed SSE/);
        return true;
      });
    },
  );
});

test("finish_reason without [DONE] still completes (routers omit DONE)", async () => {
  await withServer(
    (_req, res) => sse([text("ok"), fin("length")])(res),
    async (base) => {
      const evs = await collect(provider(base));
      const done = evs.find((e) => e.type === "done")!;
      assert.equal(done.usage.finish_reason, "length");
    },
  );
});

test("429 honors Retry-After and is retryable; 401 is permanent", async () => {
  await withServer(
    (_req, res) => {
      res.writeHead(429, { "retry-after": "7" });
      res.end("rate limited");
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.ok(
          e.retryAfterMs !== undefined &&
            e.retryAfterMs > 5_000 &&
            e.retryAfterMs <= 7_500,
          `retryAfterMs ≈ 7s, got ${e.retryAfterMs}`,
        );
        return true;
      });
    },
  );
  await withServer(
    (_req, res) => {
      res.writeHead(401);
      res.end("bad key");
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError && !e.retryable);
        assert.match(e.message, /401/);
        return true;
      });
    },
  );
});

test("5xx is retryable; a giant error body is bounded", async () => {
  await withServer(
    (_req, res) => {
      res.writeHead(500);
      res.end("x".repeat(100_000));
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError && e.retryable);
        assert.ok(e.message.length < 5_000, "error body bounded");
        return true;
      });
    },
  );
});

test("connection refused is a retryable transport error", async () => {
  // Nothing listens on this port — an ordinary provider outage.
  const p = provider("http://127.0.0.1:1");
  await assert.rejects(collect(p), (e: unknown) => {
    assert.ok(e instanceof ModelError && e.retryable);
    assert.match(e.message, /model request failed/);
    return true;
  });
});

test("request timeout is a retryable error and cleans up", async () => {
  await withServer(
    (_req, res) => {
      res.writeHead(200, { "content-type": "text/event-stream" });
      // Never writes, never ends — the wall timeout must fire.
      setTimeout(() => res.end(), 60_000).unref();
    },
    async (base) => {
      await assert.rejects(collect(provider(base, 300)), (e: unknown) => {
        assert.ok(e instanceof ModelError && e.retryable);
        return true;
      });
    },
  );
});

test("unparseable tool arguments are a retryable failure, not a call", async () => {
  await withServer(
    (_req, res) =>
      sse([
        chunk({
          choices: [
            {
              delta: {
                tool_calls: [
                  {
                    index: 0,
                    id: "c1",
                    function: { name: "journal_note", arguments: "{bad" },
                  },
                ],
              },
            },
          ],
        }),
        fin("tool_calls"),
        "[DONE]",
      ])(res),
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError && e.retryable);
        assert.match(e.message, /unparseable tool arguments/);
        return true;
      });
    },
  );
});

test("context-length refusals classify as deterministic capacity refusals", async () => {
  // HTTP 413 — authoritative even with an empty body.
  await withServer(
    (_req, res) => {
      res.writeHead(413);
      res.end();
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, false);
        assert.equal(e.refusal, "context_length");
        return true;
      });
    },
  );
  // HTTP 400 with the provider's machine-readable code.
  await withServer(
    (_req, res) => {
      res.writeHead(400);
      res.end(
        JSON.stringify({
          error: {
            code: "context_length_exceeded",
            message: "This model's maximum context length is 131072 tokens.",
          },
        }),
      );
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, false);
        assert.equal(e.refusal, "context_length");
        return true;
      });
    },
  );
  // HTTP 400 with only a message pattern — no structured code.
  await withServer(
    (_req, res) => {
      res.writeHead(400);
      res.end("maximum context length is 131072 tokens");
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.refusal, "context_length");
        return true;
      });
    },
  );
  // An in-band error chunk carrying the code.
  await withServer(
    (_req, res) =>
      sse([
        chunk({
          error: { code: "model_context_window_exceeded", message: "too long" },
        }),
        "[DONE]",
      ])(res),
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.refusal, "context_length");
        return true;
      });
    },
  );
});

test("non-capacity errors are never classified as context refusal", async () => {
  // A 429 whose body mentions tokens is still a rate limit.
  await withServer(
    (_req, res) => {
      res.writeHead(429, { "retry-after": "1" });
      res.end("rate limit: too many tokens");
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.equal(e.refusal, undefined);
        return true;
      });
    },
  );
  // An ordinary invalid request has no capacity signal.
  await withServer(
    (_req, res) => {
      res.writeHead(400);
      res.end(
        JSON.stringify({
          error: { code: "invalid_request_error", message: "bad param" },
        }),
      );
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, false);
        assert.equal(e.refusal, undefined);
        return true;
      });
    },
  );
  // A 5xx stays a plain transient failure.
  await withServer(
    (_req, res) => {
      res.writeHead(503);
      res.end("Service unavailable: try again");
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.equal(e.refusal, undefined);
        return true;
      });
    },
  );
  // A retryable status is authoritative in the non-capacity direction even
  // when bare token wording matches a refusal pattern: a codeless 429/5xx
  // phrased in tokens is throttling or a server error, never a size refusal.
  await withServer(
    (_req, res) => {
      res.writeHead(429);
      res.end(JSON.stringify({ error: { message: "too many tokens" } }));
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.equal(e.refusal, undefined);
        return true;
      });
    },
  );
  await withServer(
    (_req, res) => {
      res.writeHead(500);
      res.end(JSON.stringify({ error: { message: "token limit exceeded" } }));
    },
    async (base) => {
      await assert.rejects(collect(provider(base)), (e: unknown) => {
        assert.ok(e instanceof ModelError);
        assert.equal(e.retryable, true);
        assert.equal(e.refusal, undefined);
        return true;
      });
    },
  );
});

test('an "error": null chunk is not an error — stream completes (NF1)', async () => {
  await withServer(
    (_req, res) =>
      // LiteLLM and some OpenAI-compatible routers serialize a null
      // error field on ordinary chunks.
      sse([chunk({ error: null }), text("hello"), fin("stop"), "[DONE]"])(res),
    async (base) => {
      const evs = await collect(provider(base));
      assert.equal(
        evs
          .filter((e) => e.type === "text")
          .map((e) => e.delta)
          .join(""),
        "hello",
      );
      assert.ok(evs.some((e) => e.type === "done"));
    },
  );
});

const NOTE_TOOL = {
  name: "journal.note",
  description: "note",
  parameters: {
    type: "object",
    properties: { text: { type: "string" } },
    required: ["text"],
  },
};

const toolCallChunk = (args: string) =>
  chunk({
    choices: [
      {
        delta: {
          tool_calls: [
            {
              index: 0,
              id: "c1",
              function: { name: "journal_note", arguments: args },
            },
          ],
        },
      },
    ],
  });

test("tools are offered inside the {route, input} envelope and the chosen route is kept (ADR 0013)", async () => {
  let offered: unknown;
  await withServer(
    (req, res) => {
      let body = "";
      req.on("data", (d) => (body += d));
      req.on("end", () => {
        offered = (
          JSON.parse(body) as { tools: { function: { parameters: unknown } }[] }
        ).tools[0]?.function.parameters;
        sse([
          toolCallChunk(
            JSON.stringify({ route: "elevated", input: { text: "x" } }),
          ),
          fin("tool_calls"),
          "[DONE]",
        ])(res);
      });
    },
    async (base) => {
      const out: ModelEvent[] = [];
      for await (const ev of provider(base).stream({
        ...REQ,
        tools: [NOTE_TOOL],
      })) {
        out.push(ev);
      }
      assert.deepEqual(offered, {
        type: "object",
        additionalProperties: false,
        required: ["route", "input"],
        properties: {
          route: (offered as { properties: { route: unknown } }).properties
            .route,
          input: NOTE_TOOL.parameters,
        },
      });
      const call = out.find((e) => e.type === "tool_call");
      assert.deepEqual(call, {
        type: "tool_call",
        call: {
          id: "c1",
          name: "journal.note",
          route: "elevated",
          arguments: { text: "x" },
        },
      });
    },
  );
});

test("a call without a valid route is malformed, never treated as normal", async () => {
  for (const args of [
    { text: "x" },
    { route: "sideways", input: { text: "x" } },
    { route: "normal", input: { text: "x" }, extra: 1 },
    { route: "normal", input: ["x"] },
  ]) {
    await withServer(
      (_req, res) =>
        sse([toolCallChunk(JSON.stringify(args)), fin("tool_calls"), "[DONE]"])(
          res,
        ),
      async (base) => {
        await assert.rejects(
          (async () => {
            for await (const _ of provider(base).stream({
              ...REQ,
              tools: [NOTE_TOOL],
            })) {
              /* drain */
            }
          })(),
          (e: unknown) =>
            e instanceof ModelError &&
            e.retryable &&
            /malformed call envelope/.test(e.message),
          JSON.stringify(args),
        );
      },
    );
  }
});

test("a reserved extra header fails the call, never the credential", async () => {
  const p = new OpenAIProvider({
    baseUrl: "http://127.0.0.1:1",
    apiKey: "test-key",
    model: "test-model",
    headers: { Authorization: "Bearer spoof" },
  });
  await assert.rejects(
    collect(p),
    (e: unknown) =>
      e instanceof ModelError && !e.retryable && /reserved/.test(e.message),
  );
});

test("a replayed call keeps a valid wire name after the tool leaves the advertised set", async () => {
  let parsed: {
    messages?: {
      role: string;
      name?: string;
      tool_call_id?: string;
      tool_calls?: { id: string; function: { name: string } }[];
    }[];
  } = {};
  await withServer(
    (req, res) => {
      let body = "";
      req.on("data", (d) => (body += d));
      req.on("end", () => {
        parsed = JSON.parse(body) as typeof parsed;
        sse([fin("stop"), "[DONE]"])(res);
      });
    },
    async (base) => {
      // journal.note ran in an earlier generation; this consultation no
      // longer advertises it. The replayed canonical name must still
      // become the wire-legal form, not the raw dotted name.
      for await (const _ of provider(base).stream({
        ...REQ,
        messages: [
          {
            role: "assistant",
            content: "",
            toolCalls: [
              {
                id: "c1",
                name: "journal.note",
                route: "normal",
                arguments: { text: "x" },
              },
            ],
          },
          {
            role: "tool",
            toolCallId: "c1",
            // The secretary sets the canonical name on tool results; the
            // wire must NOT carry it — tool_call_id is the linkage and
            // some upstreams reject the extra field (observed live:
            // omen-alpha 400s '"name" is not supported by this endpoint').
            name: "journal.note",
            content: '{"ok":true}',
          },
        ],
      })) {
        /* drain */
      }
    },
  );
  const replayed = parsed.messages?.[0]?.tool_calls?.[0];
  assert.equal(replayed?.id, "c1");
  assert.equal(replayed?.function.name, "journal_note");
  const toolMsg = parsed.messages?.find((m) => m.role === "tool");
  assert.equal(toolMsg?.tool_call_id, "c1");
  assert.equal(toolMsg?.name, undefined, "tool message leaked name field");
});

test("a configured session header carries the persona's stable identity", async () => {
  const seenHeaders: Record<string, string | string[] | undefined>[] = [];
  await withServer(
    (req, res) => {
      seenHeaders.push(req.headers);
      let body = "";
      req.on("data", (d) => (body += d));
      req.on("end", () => sse([fin("stop"), "[DONE]"])(res));
    },
    async (base) => {
      const p = new OpenAIProvider({
        baseUrl: base,
        apiKey: "test-key",
        model: "test-model",
        sessionHeader: "x-opencode-session",
        // Differently-cased stale value: fetch would combine it into
        // "stale, p" — the live identity must replace it entirely.
        headers: { "X-OpenCode-Session": "stale" },
      });
      await collect(p);
      // Default honest UA; a differently-cased operator override must
      // replace it rather than combine into a comma-joined value.
      const q = new OpenAIProvider({
        baseUrl: base,
        apiKey: "test-key",
        model: "test-model",
        headers: { "user-agent": "operator-agent/1" },
      });
      await collect(q);
    },
  );
  assert.equal(seenHeaders[0]!["x-opencode-session"], "p");
  assert.equal(seenHeaders[0]!["user-agent"], "sumi-secretary/alpha");
  assert.equal(seenHeaders[1]!["user-agent"], "operator-agent/1");
});
