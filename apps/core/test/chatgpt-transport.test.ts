import assert from "node:assert/strict";
import { test } from "node:test";
import {
  ModelError,
  type ModelEvent,
  type ModelRequest,
} from "../src/provider.ts";
import { CHATGPT_REJECTED_HEADER } from "../src/providers/chatgpt-codex.ts";
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
      });
      return new Response(completed, {
        headers: { "Content-Type": "text/event-stream" },
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
          headers: { "X-Sumi-Model-Error": code },
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
    return new Response("private", { status: 401 });
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
