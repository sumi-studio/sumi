import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { OpenAIResponsesProvider } from "../src/providers/openai-responses.ts";
import { Secretary } from "../src/secretary.ts";
import { INTERNAL_TOOLS, toolSpecs } from "../src/tools.ts";

/**
 * conversation_history across the OpenAI Responses wire (hosted L1
 * acceptance, 2026-09-28). With `strict` omitted, Responses normalized the
 * tool into strict mode — every property required — and the real model
 * could only send reads carrying a query and three locators, all of which
 * the receiver rightly rejects. The contract file carries the server's own
 * echo of that normalized schema and the four real requests.
 */

type Json = Record<string, unknown>;
type Schema = Json & {
  type?: string;
  properties?: Record<string, Schema>;
  required?: string[];
  items?: Schema;
};

const CONTRACT = JSON.parse(
  readFileSync(
    new URL(
      "../../../contracts/conversation-history-fixtures.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as {
  openai_responses_strict_normalization: {
    strict: boolean;
    input_schema: Schema;
    error: string;
  };
  cases: { name: string; request: Json; ok?: boolean; error?: string }[];
};
const NORMALIZED = CONTRACT.openai_responses_strict_normalization;
const OBSERVED = CONTRACT.cases.filter((c) => c.name.startsWith("observed "));
const PERSONA = "01930e00-0000-7000-8000-0000000000c7";

/**
 * What the Responses server makes of one function tool: `strict: false`
 * keeps the schema as sent; an omitted `strict` is normalized into strict
 * mode — every object's properties all required, no additional
 * properties (documented behavior; the contract's echo pins it).
 */
function responsesView(tool: Json): { strict: boolean; parameters: Schema } {
  const params = tool.parameters as Schema;
  if (tool.strict === false) return { strict: false, parameters: params };
  const norm = (s: Schema): Schema => {
    const out: Schema = { ...s };
    if (s.properties) {
      out.properties = Object.fromEntries(
        Object.entries(s.properties).map(([k, v]) => [k, norm(v)]),
      );
      out.required = Object.keys(s.properties);
      out.additionalProperties = false;
    }
    if (s.items) out.items = norm(s.items);
    return out;
  };
  return { strict: true, parameters: norm(params) };
}

/** The JSON Schema subset these tools use; a strict decoder emits only valid values. */
function validates(s: Schema, v: unknown): boolean {
  if (Array.isArray(s.enum) && !s.enum.includes(v)) return false;
  switch (s.type) {
    case "string":
      return (
        typeof v === "string" &&
        v.length >= ((s.minLength as number) ?? 0) &&
        v.length <= ((s.maxLength as number) ?? Infinity)
      );
    case "integer":
      return (
        Number.isInteger(v) &&
        (v as number) >= ((s.minimum as number) ?? -Infinity) &&
        (v as number) <= ((s.maximum as number) ?? Infinity)
      );
    case "array":
      return (
        Array.isArray(v) && (!s.items || v.every((x) => validates(s.items!, x)))
      );
    case "object": {
      if (v === null || typeof v !== "object" || Array.isArray(v)) return false;
      const o = v as Json;
      const props = s.properties ?? {};
      if ((s.required ?? []).some((k) => !(k in o))) return false;
      if (
        s.additionalProperties === false &&
        Object.keys(o).some((k) => !(k in props))
      ) {
        return false;
      }
      return Object.entries(o).every(
        ([k, x]) => !props[k] || validates(props[k]!, x),
      );
    }
    default:
      return true;
  }
}

/** FakeState's history receiver, as a claim runs it. */
function fakeHistory(state: FakeState, request: Json): Json {
  return (
    state as unknown as { historyTool(p: string, r: Json): Json }
  ).historyTool(PERSONA, request);
}

test("contract: FakeState gives Go's verdict on every conversation_history case", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  await sealChunkOne(state); // the accepted cases also execute
  for (const c of CONTRACT.cases) {
    let err: Error | null = null;
    try {
      fakeHistory(state, structuredClone(c.request));
    } catch (e) {
      err = e as Error;
    }
    if (c.ok) {
      assert.equal(err, null, `${c.name}: want accepted, got ${err?.message}`);
    } else {
      assert.ok(err, `${c.name}: want ${c.error}, got accepted`);
      assert.equal(err.message, `bad request: ${c.error}`, c.name);
    }
  }
});

test("wire: every Responses function tool opts out of strict normalization, schema as written", async () => {
  let body: Json | null = null;
  const p = new OpenAIResponsesProvider(
    {
      baseUrl: "https://example.invalid/v1",
      apiKey: "k",
      model: "m",
      timeoutMs: 5_000,
    },
    async (_url, init) => {
      body = JSON.parse(String(init?.body)) as Json;
      return sseResponse([completedEvent([])]);
    },
  );
  const specs = INTERNAL_TOOLS.map(({ name, description, parameters }) => ({
    name,
    description,
    parameters,
  }));
  for await (const _ of p.stream({
    personaId: PERSONA,
    turnId: "t",
    round: 0,
    messages: [{ role: "user", content: "hi" }],
    tools: specs,
  })) {
    // drain
  }
  const tools = (body as Json | null)?.tools as Json[];
  assert.equal(tools.length, specs.length);
  for (const [i, t] of tools.entries()) {
    assert.equal(t.strict, false, `${t.name as string} must set strict:false`);
    const view = responsesView(t);
    const input = (view.parameters.properties as Record<string, Schema>).input;
    assert.deepEqual(
      input,
      specs[i]!.parameters,
      `${t.name as string} reaches the model as written`,
    );
  }
});

test("mechanism: the omitted-strict wire is the observed normalized schema, which admits no valid read", async () => {
  const history = toolSpecs().find((t) => t.name === "conversation_history")!;
  // The tool exactly as the pre-fix provider sent it: no `strict` field.
  const { strict: _ignored, ...prefix } = (
    await serializedTools([history])
  )[0]!;
  const view = responsesView(prefix);
  const input = (view.parameters.properties as Record<string, Schema>).input!;
  // The emulated normalization is the real server's echo, field for field.
  assert.equal(view.strict, NORMALIZED.strict);
  assert.deepEqual(input, NORMALIZED.input_schema);

  const state = new FakeState();
  state.addPersona(PERSONA);
  // The real model's four calls were schema-valid under that view, and
  // every one is a bad request at the receiver.
  assert.equal(OBSERVED.length, 4);
  for (const c of OBSERVED) {
    assert.ok(
      validates(input, c.request),
      `${c.name} is schema-valid under the view`,
    );
    assert.throws(() => fakeHistory(state, structuredClone(c.request)), {
      message: `bad request: ${NORMALIZED.error}`,
    });
  }
  // No schema-valid read exists: the view requires a non-empty query, and
  // the receiver refuses any read that carries one. The read the model
  // needed cannot be emitted at all.
  assert.ok(input.required!.includes("query"));
  assert.equal((input.properties!.query as Schema).minLength, 1);
  assert.equal(validates(input, { operation: "read", chunk_seq: 1 }), false);
  // Under strict:false the same read is valid as written.
  const fixed = responsesView((await serializedTools([history]))[0]!);
  const fixedInput = (fixed.parameters.properties as Record<string, Schema>)
    .input!;
  assert.equal(
    validates(fixedInput, { operation: "read", chunk_seq: 1 }),
    true,
  );
  assert.equal(
    validates(fixedInput, { operation: "read", chunk_seq: 1, query: "" }),
    false,
  );
});

for (const variant of [
  "omitted strict (before)",
  "strict:false (after)",
] as const) {
  test(`end to end, ${variant}: Q2's chunk read through the Responses provider into the receiver`, async () => {
    const state = new FakeState();
    state.addPersona(PERSONA);
    await sealChunkOne(state);
    const toolResults: Json[] = [];
    let round = 0;
    // A Responses emulator: resolves the tool's view like the server, and
    // plays a strict-decoding model — the read it wants if the schema
    // admits it, else the call the real model actually sent.
    const provider = new OpenAIResponsesProvider(
      {
        baseUrl: "https://example.invalid/v1",
        apiKey: "k",
        model: "m",
        timeoutMs: 5_000,
      },
      async (_url, init) => {
        const body = JSON.parse(String(init?.body)) as Json;
        const tools = body.tools as Json[];
        const wire = tools.find((t) => t.name === "conversation_history")!;
        if (variant.startsWith("omitted")) delete wire.strict; // the pre-fix serializer
        round++;
        if (round > 1) {
          const out = (body.input as Json[]).filter(
            (i) => i.type === "function_call_output",
          );
          toolResults.push(...out);
          return sseResponse([completedEvent([messageItem("done")])]);
        }
        const view = responsesView(wire);
        const input = (view.parameters.properties as Record<string, Schema>)
          .input!;
        const wanted = { operation: "read", chunk_seq: 1 };
        const sent = validates(input, wanted) ? wanted : OBSERVED[0]!.request;
        assert.ok(
          validates(input, sent),
          "a strict decoder only emits schema-valid calls",
        );
        const args = JSON.stringify({ route: "normal", input: sent });
        return sseResponse([
          completedEvent([callItem("conversation_history", args)]),
        ]);
      },
    );
    const s = new Secretary({
      personaId: PERSONA,
      holderId: "history-contract",
      state,
      provider,
      leaseTtlMs: 30_000,
      renewEveryMs: 1_000,
      contextLimit: 200_000,
      pollIntervalMs: 1,
      scheduleEveryMs: 60_000,
      idgen: () => crypto.randomUUID(),
    });
    await s.start();
    state.addInput(PERSONA, "q2", "open the original records of chunk 1");
    assert.equal(await s.step(), "turn");
    await s.stop();

    const result = state.eventLog.find(
      (e) =>
        e.kind === "tool_result" && e.payload.tool === "conversation_history",
    );
    assert.ok(result, "the call reached the receiver and was recorded");
    const recorded = JSON.stringify(result.payload);
    const fed = JSON.stringify(toolResults);
    if (variant.startsWith("omitted")) {
      assert.match(recorded, new RegExp(NORMALIZED.error));
      assert.doesNotMatch(recorded, /ORIGINAL-PLAN-MARKER/);
    } else {
      assert.doesNotMatch(recorded, /bad request/);
      assert.match(
        recorded,
        /ORIGINAL-PLAN-MARKER/,
        "the chunk's original record came back",
      );
      assert.match(
        fed,
        /ORIGINAL-PLAN-MARKER/,
        "and was fed to the model as the call output",
      );
    }
  });
}

// ---------------------------------------------------------------- helpers ---

async function serializedTools(
  specs: ReturnType<typeof toolSpecs>,
): Promise<Json[]> {
  let body: Json | null = null;
  const p = new OpenAIResponsesProvider(
    {
      baseUrl: "https://example.invalid/v1",
      apiKey: "k",
      model: "m",
      timeoutMs: 5_000,
    },
    async (_url, init) => {
      body = JSON.parse(String(init?.body)) as Json;
      return sseResponse([completedEvent([])]);
    },
  );
  for await (const _ of p.stream({
    personaId: PERSONA,
    turnId: "t",
    round: 0,
    messages: [{ role: "user", content: "hi" }],
    tools: specs,
  })) {
    // drain
  }
  return (body as Json | null)?.tools as Json[];
}

/** Chunk 1 sealed over a turn whose first input carries a marker. */
async function sealChunkOne(state: FakeState) {
  const lease = await state.acquireWriter(PERSONA, "history-contract", 30_000);
  const big = "x".repeat(41_000);
  const evs: { kind: string; payload: Json }[] = [
    {
      kind: "input_received",
      payload: {
        input_id: "m1",
        kind: "message",
        payload: { text: "ORIGINAL-PLAN-MARKER: hold 19:00 at Lume" },
        actor_kind: "human",
      },
    },
    { kind: "assistant_message", payload: { text: big } },
    { kind: "assistant_message", payload: { text: big } },
    {
      kind: "input_received",
      payload: {
        input_id: "m2",
        kind: "message",
        payload: { text: "later" },
        actor_kind: "human",
      },
    },
    { kind: "assistant_message", payload: { text: "ok" } },
  ];
  for (const [i, e] of evs.entries()) {
    state.eventLog.push({
      persona_id: PERSONA,
      seq: i + 1,
      turn_id: "t",
      kind: e.kind,
      payload: e.payload,
      created_at: "2026-09-28T00:00:00.000Z",
    });
  }
  await state.memoryMaintain(PERSONA, lease.generation);
  assert.equal(state.memoryChunks[0]?.chunk_seq, 1, "chunk 1 sealed");
}

function sseResponse(events: Json[]): Response {
  return new Response(
    events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join(""),
    {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    },
  );
}

function completedEvent(output: Json[]): Json {
  return {
    type: "response.completed",
    response: {
      status: "completed",
      output,
      usage: { input_tokens: 5, output_tokens: 3 },
    },
  };
}

function callItem(name: string, args: string): Json {
  return {
    type: "function_call",
    id: "fc_1",
    call_id: "call_1",
    name,
    arguments: args,
  };
}

function messageItem(text: string): Json {
  return {
    type: "message",
    role: "assistant",
    content: [{ type: "output_text", text }],
  };
}
