import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";
import type { MemorySnapshot } from "../src/memory-branch.ts";

test("real Postgres snapshot roundtrip preserves the actual Responses wire prefix and schema key order", {
  skip: !process.env.SUMI_MEMORY_PREFIX_PROOF,
}, async () => {
  const proof = JSON.parse(
    await readFile(process.env.SUMI_MEMORY_PREFIX_PROOF!, "utf8"),
  ) as { before: MemorySnapshot; after: MemorySnapshot };
  const bodies: Record<string, unknown>[] = [];
  const provider = new OpenAIResponsesProvider(
    {
      baseUrl: "https://example.invalid/v1",
      apiKey: "fixture",
      model: "gpt-6-astra",
      extra: { reasoning: { effort: "high" } },
    },
    async (_u, init) => {
      bodies.push(JSON.parse(String(init?.body)));
      return new Response(
        'data: {"type":"response.completed","response":{"status":"completed","output":[],"usage":{}}}\n\n',
        { headers: { "content-type": "text/event-stream" } },
      );
    },
  );
  const run = async (snapshot: MemorySnapshot, branch: boolean) => {
    for await (const _ of provider.stream({
      personaId: "proof",
      generation: 1,
      turnId: "proof",
      round: 0,
      phase: branch ? "memory" : "turn",
      messages: branch
        ? [
            ...snapshot.messages,
            { role: "user", content: "organize this range" },
          ]
        : snapshot.messages,
      tools: snapshot.tools,
      ...(branch
        ? {
            reasoningEffort: "medium" as const,
            reasoningEffortAfter: snapshot.messages.length,
          }
        : {}),
    })) {
      /*consume*/
    }
  };
  await run(proof.before, false);
  await run(proof.after, true);
  const [a, b] = bodies as [Record<string, unknown>, Record<string, unknown>];
  const input = a.input as unknown[];
  assert.equal(JSON.stringify(a.tools), JSON.stringify(b.tools));
  assert.equal(a.instructions, b.instructions);
  assert.equal(
    JSON.stringify(input),
    JSON.stringify((b.input as unknown[]).slice(0, input.length)),
  );
  assert.equal(JSON.stringify(a.reasoning), JSON.stringify(b.reasoning));
  assert.ok(
    JSON.stringify(a.tools).indexOf('"z"') <
      JSON.stringify(a.tools).indexOf('"a"'),
  );
});
