import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import {
  ModelError,
  type ModelProvider,
  type ModelRequest,
} from "../src/provider.ts";
import { continuationScope } from "../src/providers/chatgpt-codex.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";
import type { ModelBinding } from "../src/types.ts";

const persona = "01930e00-0000-7000-8000-0000000000c1";
type WireBody = Record<string, unknown> & {
  input: Record<string, unknown>[];
};
const parent: ModelRequest = {
  personaId: persona,
  turnId: "turn-1",
  round: 0,
  generation: 1,
  messages: [
    { role: "system", content: "You are Sumi." },
    {
      role: "user",
      content: "We have not decided whether to move the meeting.",
    },
    { role: "assistant", content: "The original time still stands." },
  ],
  tools: [
    {
      name: "file.write",
      description: "Write a file.",
      parameters: { type: "object" },
    },
    {
      name: "file.read",
      description: "Read a file.",
      parameters: { type: "object" },
    },
    {
      name: "messaging.send",
      description: "Send a message.",
      parameters: { type: "object" },
    },
  ],
};

async function consume(provider: ModelProvider, request = parent) {
  const out = [];
  for await (const ev of provider.stream(request)) out.push(ev);
  return out;
}

function response() {
  return new Response(
    `data: ${JSON.stringify({
      type: "response.completed",
      response: {
        status: "completed",
        output: [],
        usage: { input_tokens: 25, output_tokens: 1 },
      },
    })}\n\n`,
    { headers: { "content-type": "text/event-stream" } },
  );
}

function config() {
  return {
    baseUrl: "https://example.invalid/v1",
    apiKey: "private-api-key",
    model: "gpt-6-astra",
    extra: { reasoning: { effort: "high" } },
    headers: { "X-Private-Routing": "private-routing-value" },
  };
}

test("a memory effort update preserves the parent's wire prefix, tools, and original effort across rounds", async () => {
  const bodies: WireBody[] = [];
  const provider = new OpenAIResponsesProvider(config(), async (_url, init) => {
    bodies.push(JSON.parse(String(init?.body)));
    return response();
  });
  const snapshot = await provider.snapshotBinding();
  await consume(provider, { ...parent, bindingSnapshot: snapshot });
  const memory: ModelRequest = {
    ...parent,
    phase: "memory",
    bindingSnapshot: snapshot,
    reasoningEffort: "medium",
    reasoningEffortAfter: parent.messages.length,
    messages: [
      ...parent.messages,
      { role: "user", content: "Prepare a reviewed memory draft." },
    ],
  };
  await consume(provider, memory);
  await consume(provider, {
    ...memory,
    round: 1,
    messages: [
      ...memory.messages,
      { role: "assistant", content: "Reviewing the draft." },
      {
        role: "user",
        content: "Compare it with the source before finalizing.",
      },
    ],
  });
  const [original, firstBranch, secondBranch] = bodies;
  assert.ok(original && firstBranch && secondBranch);
  for (const branch of bodies.slice(1)) {
    assert.deepEqual(branch.tools, original.tools);
    assert.equal(branch.instructions, original.instructions);
    assert.deepEqual(branch.reasoning, original.reasoning);
    assert.equal(branch.prompt_cache_key, original.prompt_cache_key);
    assert.equal(branch.text, undefined);
    assert.deepEqual(
      branch.input.slice(0, original.input.length),
      original.input,
    );
    assert.deepEqual(branch.input[original.input.length], {
      type: "configuration_update",
      reasoning: { effort: "medium" },
    });
    assert.equal(
      branch.input.filter((i) => i.type === "configuration_update").length,
      1,
    );
  }
  assert.deepEqual(
    secondBranch.input.slice(0, firstBranch.input.length),
    firstBranch.input,
  );
});

test("the subscription lite wire keeps the same prefix when memory changes effort", async () => {
  const bodies: WireBody[] = [];
  const provider = new OpenAIResponsesProvider({
    baseUrl: "https://chatgpt.com/backend-api/codex",
    apiKey: "",
    model: "gpt-6-astra",
    chatgpt: {
      accountId: "private-account",
      reasoningEffort: "high",
      send: async (body) => {
        bodies.push(JSON.parse(body));
        return response();
      },
    },
  });
  await consume(provider);
  await consume(provider, {
    ...parent,
    phase: "memory",
    reasoningEffort: "medium",
    reasoningEffortAfter: parent.messages.length,
    messages: [
      ...parent.messages,
      { role: "user", content: "Prepare the memory." },
    ],
  });
  const [original, branch] = bodies;
  assert.ok(original && branch);
  assert.deepEqual(
    branch.input.slice(0, original.input.length),
    original.input,
  );
  assert.deepEqual(branch.reasoning, original.reasoning);
  assert.deepEqual(branch.input[original.input.length], {
    type: "configuration_update",
    reasoning: { effort: "medium" },
  });
});

test("memory rounds with reused call IDs keep the frozen native history and effort boundary", async () => {
  for (const subscription of [false, true]) {
    const bodies: WireBody[] = [];
    const record = (body: string) => {
      bodies.push(JSON.parse(body));
      return Promise.resolve(response());
    };
    const provider = subscription
      ? new OpenAIResponsesProvider({
          ...config(),
          baseUrl: "https://chatgpt.com/backend-api/codex",
          chatgpt: {
            accountId: "private-account",
            reasoningEffort: "high",
            send: record,
          },
        })
      : new OpenAIResponsesProvider(config(), (_url, init) =>
          record(String(init?.body)),
        );
    const call = {
      id: "repeated",
      name: "file.read",
      route: "normal" as const,
      arguments: { path: "notes.md" },
    };
    const frozen: ModelRequest = {
      ...parent,
      bindingSnapshot: await provider.snapshotBinding(),
      messages: [
        ...parent.messages,
        { role: "assistant", content: "", toolCalls: [call] },
        { role: "tool", content: "Earlier note", toolCallId: call.id },
        { role: "user", content: "Read it again." },
        { role: "assistant", content: "", toolCalls: [call] },
        { role: "tool", content: "Updated note", toolCallId: call.id },
      ],
    };
    await consume(provider, frozen);
    const branch: ModelRequest = {
      ...frozen,
      phase: "memory",
      reasoningEffort: "medium",
      reasoningEffortAfter: frozen.messages.length,
      messages: [
        ...frozen.messages,
        { role: "user", content: "Review the private memory source." },
        { role: "assistant", content: "", toolCalls: [call] },
        { role: "tool", content: "Private source", toolCallId: call.id },
      ],
    };
    await consume(provider, branch);
    const [before, after] = bodies;
    assert.ok(before && after);
    assert.equal(
      JSON.stringify(after.input.slice(0, before.input.length)),
      JSON.stringify(before.input),
    );
    assert.deepEqual(after.input[before.input.length], {
      type: "configuration_update",
      reasoning: { effort: "medium" },
    });
    const ids = after.input
      .filter((item) => item.type === "function_call")
      .map((item) => item.call_id);
    assert.equal(new Set(ids).size, 3);
    assert.deepEqual(
      after.input
        .filter((item) => item.type === "function_call_output")
        .map((item) => item.call_id),
      ids,
    );
    assert.equal(JSON.stringify(after.tools), JSON.stringify(before.tools));
  }
});

test("invalid or unsupported effort updates fail before contacting a provider", async () => {
  let calls = 0;
  const send = async () => {
    calls++;
    return response();
  };
  const provider = new OpenAIResponsesProvider(config(), send);
  for (const position of [-1, 1.5, parent.messages.length + 1, undefined]) {
    await assert.rejects(
      consume(provider, {
        ...parent,
        phase: "memory",
        reasoningEffort: "medium",
        reasoningEffortAfter: position,
      }),
      (e: unknown) => e instanceof ModelError && e.unavailable === true,
    );
  }
  const other = new OpenAIResponsesProvider(
    { ...config(), model: "gpt-5.6-luna" },
    send,
  );
  await assert.rejects(
    consume(other, {
      ...parent,
      phase: "memory",
      reasoningEffort: "medium",
      reasoningEffortAfter: 3,
    }),
    /invalid memory reasoning/,
  );
  assert.equal(calls, 0);
});

test("a refused pinned or memory request does not retry with its inherited reasoning removed", async () => {
  const accountId = "private-account";
  const scope = await continuationScope(accountId, "gpt-6-astra");
  for (const mode of ["memory", "pinned-parent"] as const) {
    let calls = 0;
    const provider = new OpenAIResponsesProvider({
      baseUrl: "https://chatgpt.com/backend-api/codex",
      apiKey: "",
      model: "gpt-6-astra",
      chatgpt: {
        accountId,
        send: async (body) => {
          calls++;
          assert.match(body, /inherited-encrypted-state/);
          return new Response(
            JSON.stringify({
              error: {
                message: "configuration_update refused",
                code: "invalid_request_error",
              },
            }),
            {
              status: 400,
              headers: { "content-type": "application/json" },
            },
          );
        },
      },
    });
    await assert.rejects(
      consume(provider, {
        ...parent,
        ...(mode === "memory"
          ? { phase: "memory" as const }
          : { bindingSnapshot: await provider.snapshotBinding() }),
        messages: [
          ...parent.messages,
          {
            role: "assistant",
            content: "",
            continuation: {
              scope,
              output: [
                {
                  type: "reasoning",
                  id: "rs_parent",
                  encrypted_content: "inherited-encrypted-state",
                },
              ],
            },
          },
          { role: "user", content: "Continue from this exact context." },
        ],
      }),
      /configuration_update refused/,
    );
    assert.equal(calls, 1, mode);
  }
});

test("saved model configuration has no credentials and refuses a changed configuration before sending", async () => {
  let calls = 0;
  const send = async () => {
    calls++;
    return response();
  };
  const original = new OpenAIResponsesProvider(config(), send);
  const snapshot = await original.snapshotBinding();
  assert.equal(snapshot.model, "gpt-6-astra");
  assert.equal(snapshot.reasoningEffort, "high");
  assert.equal(snapshot.fingerprint.length, 64);
  assert.doesNotMatch(
    JSON.stringify(snapshot),
    /private-api-key|private-routing-value|example.invalid/,
  );
  const resumed = new OpenAIResponsesProvider(config(), send);
  await consume(resumed, { ...parent, bindingSnapshot: snapshot });
  assert.equal(calls, 1);
  for (const change of [
    { apiKey: "different-account-key" },
    { model: "gpt-5.6-luna" },
    { baseUrl: "https://different.invalid/v1" },
    { extra: { reasoning: { effort: "medium" } } },
    { headers: { "X-Private-Routing": "different" } },
    { maxOutputTokens: 4000 },
  ]) {
    const changed = new OpenAIResponsesProvider(
      { ...config(), ...change },
      send,
    );
    await assert.rejects(
      consume(changed, { ...parent, bindingSnapshot: snapshot }),
      (error: unknown) =>
        error instanceof ModelError &&
        error.cause === "model_binding_changed" &&
        error.unavailable === true,
    );
  }
  assert.equal(calls, 1);
});

test("a saved selected connection cannot silently change while its branch is stopped", async () => {
  const state = new FakeState();
  state.addPersona(persona);
  const lease = await state.acquireWriter(persona, "writer", 60000);
  const original: ModelBinding = {
    selection: "api",
    credential_available: true,
    api_key: "secret",
    connection: {
      id: "connection-1",
      name: "My account",
      version: "1",
      preset: "openai-responses",
      base_url: "https://example.invalid/v1",
      model: "gpt-6-astra",
    },
  };
  state.setModelBinding(persona, original);
  let admissions = 0;
  const admit = state.admitUsage.bind(state);
  state.admitUsage = async (...args) => {
    admissions++;
    return admit(...args);
  };
  const selected = () =>
    new SelectedModelProvider({
      state,
      persona,
      fallback: new MockProvider(),
      timeoutMs: 1000,
    });
  const snapshot = await selected().snapshotBinding();
  assert.ok(original.connection);
  assert.doesNotMatch(JSON.stringify(snapshot), /secret|connection-1/);
  for (const change of [
    { id: "connection-2" },
    { version: "2" },
    { model: "gpt-5.6-luna" },
    { base_url: "https://different.invalid/v1" },
  ]) {
    state.setModelBinding(persona, {
      ...original,
      connection: { ...original.connection, ...change },
    });
    await assert.rejects(
      consume(selected(), {
        ...parent,
        generation: lease.generation,
        bindingSnapshot: snapshot,
      }),
      (error: unknown) =>
        error instanceof ModelError &&
        error.cause === "model_binding_changed" &&
        error.unavailable === true,
    );
  }
  assert.equal(admissions, 0);
  state.setModelBinding(persona, original);
  assert.deepEqual(await selected().snapshotBinding(), snapshot);
});
