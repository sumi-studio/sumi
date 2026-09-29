import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  advanceMemoryBranch,
  DEFAULT_MEMORY_POLICY,
  initialMemoryState,
  type MemoryBranch,
} from "../src/memory-branch.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { Secretary } from "../src/secretary.ts";
import { SKILL_CATALOG } from "../src/skills.ts";
import { INTERNAL_TOOLS, toolSpecs } from "../src/tools.ts";

const persona = "01930e00-0000-7000-8000-000000000071";
const docs = new URL("../../api/internal/agentstate/skills/", import.meta.url);
const guide = (name: string) =>
  readFileSync(new URL(`${name}.md`, docs), "utf8");

test("the model catalog names exactly the bundled guides, without loading their bodies", () => {
  const spec = toolSpecs(new Set(["skill.read"]))[0];
  assert.ok(spec);
  assert.equal(spec.name, "skill.read");
  assert.deepEqual(
    SKILL_CATALOG.map(({ name }) => `${name}.md`).sort(),
    readdirSync(docs).sort(),
  );
  for (const { name, description } of SKILL_CATALOG) {
    assert.ok(spec.description.includes(`${name}: ${description}`));
    assert.ok(!spec.description.includes(guide(name)));
  }
  assert.ok(!toolSpecs(new Set()).some((tool) => tool.name === "skill.read"));
});

test("a secretary can discover and read a guide, then receives its durable result in context", async () => {
  const state = new FakeState();
  state.addPersona(persona);
  state.addInput(
    persona,
    "attachment-help",
    "添付ファイルを送りたい。使い方を確認して。",
  );
  const body = guide("messaging");
  let reads = 0;
  // Core-side fixture for the API's reader. The Go tests exercise the
  // actual embedded reader and its PostgreSQL operation receipt.
  state.registerEffect("skill.read", (_persona, _key, request) => {
    assert.deepEqual(request, { name: "messaging" });
    reads++;
    return {
      name: "messaging",
      content: body,
      media_type: "text/markdown",
      revision: "fixture",
    };
  });
  const requests: ModelRequest[] = [];
  const provider: ModelProvider = {
    name: "guide-reader",
    async *stream(request): AsyncIterable<ModelEvent> {
      requests.push(structuredClone(request));
      if (request.round === 0) {
        assert.ok(
          request.tools.some(
            (tool) =>
              tool.name === "skill.read" &&
              tool.description.includes("messaging:"),
          ),
        );
        assert.ok(
          !request.messages.some((message) => message.content.includes(body)),
        );
        yield {
          type: "tool_call",
          call: {
            id: "read-guide",
            name: "skill.read",
            route: "normal",
            arguments: { name: "messaging" },
          },
        };
      } else {
        const read = request.messages.find(
          (message) => message.role === "tool" && message.name === "skill.read",
        );
        assert.ok(read);
        assert.equal(JSON.parse(read.content).content, body);
        yield {
          type: "text",
          delta: "添付はアップロード後に投稿へ結びつけるんだね。",
        };
      }
      yield { type: "done", usage: {} };
    },
  };
  const life = new Secretary({
    personaId: persona,
    holderId: "skill-test",
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 20,
    pollIntervalMs: 1,
    scheduleEveryMs: 0,
    idgen: () => crypto.randomUUID(),
  });
  await life.start();
  try {
    assert.equal(await life.step(), "turn");
    assert.equal(reads, 1);
    assert.equal(requests.length, 2);
    const events = await state.events(persona, 0);
    const recorded = events.filter((event) => event.kind === "tool_result");
    assert.equal(recorded.length, 1);
    assert.ok(
      JSON.stringify(recorded[0]).includes(JSON.stringify(body).slice(1, -1)),
    );
  } finally {
    await life.stop();
  }
});

test("a frozen memory branch keeps the catalog but cannot read a skill", async () => {
  const branch: MemoryBranch = {
    chunk: {
      persona_id: persona,
      chunk_seq: 1,
      layer: 1,
      sources: [],
      first_seq: 1,
      last_seq: 1,
      est_tokens: 1000,
      status: "sealed",
      replacement: null,
      replacement_est_tokens: null,
      attempts: 0,
      interruptions: 0,
      last_error: null,
      claimed_generation: null,
      claimed_at: null,
      not_before: null,
      created_at: "2026-09-30T00:00:00Z",
      prepared_at: null,
      applied_at: null,
    },
    snapshot: {
      messages: [{ role: "user", content: "今ある文脈だけで整理する。" }],
      tools: INTERNAL_TOOLS.filter((tool) =>
        ["skill.read", "file.read", "file.write"].includes(tool.name),
      ),
      ranges: [{ first_seq: 1, last_seq: 1, message_index: 0 }],
      binding: { version: 1, provider: "fixture", fingerprint: "skill-test" },
    },
    state: null,
    revision: 0,
  };
  branch.state = initialMemoryState(branch, DEFAULT_MEMORY_POLICY);
  const frozen = structuredClone(branch.snapshot);
  const next = await advanceMemoryBranch(branch, {
    text: "",
    calls: [
      {
        id: "outside-read",
        name: "skill.read",
        route: "normal",
        arguments: { name: "messaging" },
      },
    ],
    usage: {},
  });
  assert.deepEqual(branch.snapshot, frozen);
  const result = next.messages.find(
    (message) => message.role === "tool" && message.name === "skill.read",
  );
  assert.ok(result);
  assert.equal(
    JSON.parse(result.content).error,
    "memory_branch_permission_denied",
  );
  assert.ok(!JSON.stringify(next).includes(guide("messaging")));
});
