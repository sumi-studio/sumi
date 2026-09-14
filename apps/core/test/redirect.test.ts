import assert from "node:assert/strict";
import http from "node:http";
import { test } from "node:test";
import { ModelError, type ModelEvent } from "../src/provider.ts";
import { AnthropicProvider } from "../src/providers/anthropic.ts";
import { OpenAIProvider } from "../src/providers/openai.ts";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";

/**
 * Redirect refusal, witnessed end-to-end: each adapter runs its real
 * `fetch` (no injected implementation) against a loopback origin that
 * answers 302 to a second loopback server. The guarantee under test:
 * the second destination never sees a request — credentials configured
 * for the selected endpoint cannot leak across a redirect — and the
 * observed 3xx fails deterministically instead of consuming the
 * transient retry budget. `redirect: "error"` is deliberately not the
 * mechanism: workerd rejects the value outright and undici reports it
 * as a generic `TypeError: fetch failed` indistinguishable from a
 * transient transport loss; `redirect: "manual"` surfaces the 3xx for
 * explicit classification on both runtimes.
 */

function req() {
  return {
    personaId: "p",
    turnId: "t",
    round: 0,
    messages: [{ role: "user" as const, content: "hi" }],
    tools: [],
  };
}

interface Rig {
  baseUrl: string;
  targetHits: http.IncomingMessage[];
  close(): Promise<void>;
}

/** origin 302s to target; `location` controls where the redirect points. */
async function redirectingOrigin(
  location: (targetPort: number, originPort: number) => string,
): Promise<Rig> {
  const targetHits: http.IncomingMessage[] = [];
  const target = http.createServer((req, res) => {
    targetHits.push(req);
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end("data: {}\n\n");
  });
  await new Promise<void>((r) => target.listen(0, "127.0.0.1", r));
  const tport = (target.address() as { port: number }).port;
  const origin = http.createServer((_req, res) => {
    const oport = (origin.address() as { port: number }).port;
    res.writeHead(302, { Location: location(tport, oport) });
    res.end();
  });
  await new Promise<void>((r) => origin.listen(0, "127.0.0.1", r));
  const oport = (origin.address() as { port: number }).port;
  return {
    baseUrl: `http://127.0.0.1:${oport}`,
    targetHits,
    async close() {
      await new Promise<void>((r) => origin.close(() => r()));
      await new Promise<void>((r) => target.close(() => r()));
    },
  };
}

async function expectRefusal(
  stream: AsyncIterable<ModelEvent>,
  targetHits: http.IncomingMessage[],
): Promise<void> {
  const err = await (async () => {
    try {
      for await (const _ of stream) void _;
      return null;
    } catch (e) {
      return e;
    }
  })();
  assert.ok(err instanceof ModelError, `expected ModelError, got ${err}`);
  assert.equal(err.retryable, false, "redirect refusal must be terminal");
  assert.match(err.message, /redirect/i);
  assert.doesNotMatch(err.message, /test-key|anthropic-key|gw-secret/);
  assert.equal(targetHits.length, 0, "redirect target must see no request");
}

for (const [name, make] of [
  [
    "chat-completions",
    (baseUrl: string) =>
      new OpenAIProvider({
        baseUrl,
        apiKey: "test-key",
        model: "m",
        headers: { "X-Gateway-Session": "gw-secret" },
      }),
  ],
  [
    "openai-responses",
    (baseUrl: string) =>
      new OpenAIResponsesProvider({
        baseUrl,
        apiKey: "test-key",
        model: "m",
        headers: { "X-Gateway-Session": "gw-secret" },
      }),
  ],
  [
    "anthropic",
    (baseUrl: string) =>
      new AnthropicProvider({
        baseUrl,
        apiKey: "anthropic-key",
        model: "m",
        headers: { "X-Gateway-Session": "gw-secret" },
      }),
  ],
] as const) {
  test(`${name}: cross-origin redirect refused, target sees nothing`, async () => {
    const rig = await redirectingOrigin(
      (tport) => `http://127.0.0.1:${tport}/`,
    );
    try {
      await expectRefusal(make(rig.baseUrl).stream(req()), rig.targetHits);
    } finally {
      await rig.close();
    }
  });
}

test("same-origin redirect is refused identically", async () => {
  const rig = await redirectingOrigin(
    (_tport, oport) => `http://127.0.0.1:${oport}/other-path`,
  );
  try {
    await expectRefusal(
      new OpenAIProvider({
        baseUrl: rig.baseUrl,
        apiKey: "test-key",
        model: "m",
      }).stream(req()),
      rig.targetHits,
    );
  } finally {
    await rig.close();
  }
});

test("a non-redirect failure still reaches httpError classification", async () => {
  // Guard against the refusal check swallowing ordinary failures: a 429
  // remains a retryable rate-limit error, not a redirect refusal.
  const srv = http.createServer((_req, res) => {
    res.writeHead(429, { "retry-after": "5" });
    res.end("slow down");
  });
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", r));
  const baseUrl = `http://127.0.0.1:${(srv.address() as { port: number }).port}`;
  try {
    const err = await (async () => {
      try {
        for await (const _ of new OpenAIProvider({
          baseUrl,
          apiKey: "test-key",
          model: "m",
        }).stream(req()))
          void _;
        return null;
      } catch (e) {
        return e;
      }
    })();
    assert.ok(err instanceof ModelError);
    assert.equal(err.retryable, true);
    assert.equal(err.retryAfterMs, 5000);
  } finally {
    await new Promise<void>((r) => srv.close(() => r()));
  }
});
