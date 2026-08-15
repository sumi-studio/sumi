import assert from "node:assert/strict";
import test from "node:test";
import {
  executorAuthorityProbeFile,
  firstProviderResponse,
  firstUserMessage,
  LoopbackChatProvider,
  secondProviderResponse,
  secondUserMessage,
} from "../e2e/support/real-agent-stack";

const providerToolCall = {
  id: "call-real-agent-list-dir",
  type: "function",
  function: {
    name: "list_dir",
    arguments: JSON.stringify({ route: "normal", input: { path: "." } }),
  },
};

async function completion(
  provider: LoopbackChatProvider,
  messages: unknown[],
  tools: unknown[] = [],
): Promise<Response> {
  return fetch(`${provider.url}/chat/completions`, {
    method: "POST",
    headers: {
      Authorization: "Bearer test-provider-key",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "kimi-k2.7-code",
      stream: true,
      messages,
      tools,
    }),
  });
}

test("loopback provider counts only requests that pass transport validation", async () => {
  const provider = new LoopbackChatProvider("test-provider-key");
  await provider.start();
  try {
    assert.equal((await fetch(`${provider.url}/probe`)).status, 404);
    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        })
      ).status,
      401,
    );
    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: "Bearer test-provider-key",
            "Content-Type": "text/plain",
          },
          body: "{}",
        })
      ).status,
      415,
    );
    assert.equal(provider.requestCount, 0);

    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: "Bearer test-provider-key",
            "Content-Type": "application/json",
          },
          body: "{}",
        })
      ).status,
      422,
    );
    assert.equal(provider.requestCount, 1);
  } finally {
    await provider.stop();
  }
});

test("loopback provider requires an exact Normal list_dir result before continuing", async () => {
  const provider = new LoopbackChatProvider("test-provider-key");
  await provider.start();
  try {
    const messages: unknown[] = [{ role: "user", content: firstUserMessage }];
    const tools = [
      {
        type: "function",
        function: {
          name: "list_dir",
          parameters: {
            type: "object",
            properties: {
              route: { type: "string", enum: ["normal", "elevated"] },
              input: {
                type: "object",
                properties: { path: { type: "string" } },
              },
            },
          },
        },
      },
    ];
    const toolCallResponse = await completion(provider, messages, tools);
    assert.equal(toolCallResponse.status, 200);
    assert.match(await toolCallResponse.text(), /call-real-agent-list-dir/);

    messages.push(
      { role: "assistant", tool_calls: [providerToolCall] },
      {
        role: "tool",
        tool_call_id: providerToolCall.id,
        content: executorAuthorityProbeFile,
      },
    );
    const firstTextResponse = await completion(provider, messages);
    assert.equal(firstTextResponse.status, 200);
    assert.match(
      await firstTextResponse.text(),
      new RegExp(firstProviderResponse),
    );
    assert.equal(provider.executorToolVerified, true);

    messages.push(
      { role: "assistant", content: firstProviderResponse },
      { role: "user", content: secondUserMessage },
    );
    const secondTextResponse = await completion(provider, messages);
    assert.equal(secondTextResponse.status, 200);
    assert.match(
      await secondTextResponse.text(),
      new RegExp(secondProviderResponse),
    );
    assert.equal(provider.requestCount, 3);
    assert.equal(provider.contextVerified, true);
  } finally {
    await provider.stop();
  }
});
