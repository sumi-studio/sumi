// Invoked by Go's synthetic integration test with its isolated testdb/API.
import assert from "node:assert/strict";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { Secretary } from "../src/secretary.ts";
import { HttpStateClient } from "../src/state-client.ts";

const [url, token, persona] = process.argv.slice(2);
assert.ok(url && token && persona);
assert.ok(url.startsWith("http://127.0.0.1:"));
const state = new HttpStateClient(url, token);
const binding = await state.modelBinding(persona);
assert.equal(binding.api_key, undefined, "subscription token must stay on Go");
assert.equal(binding.credential_available, true);
const provider = new SelectedModelProvider({
  state,
  persona,
  fallback: new MockProvider(),
  timeoutMs: 10_000,
});
const submitted = await fetch(
  `${url}/internal/core/personas/${persona}/inputs`,
  {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      input_id: crypto.randomUUID(),
      kind: "message",
      payload: { text: "Remember the synthetic transport note, then answer." },
    }),
  },
);
assert.equal(submitted.status, 201);
const secretary = new Secretary({
  personaId: persona,
  holderId: "transport-integration",
  state,
  provider,
  leaseTtlMs: 30_000,
  renewEveryMs: 1_000,
  contextLimit: 20,
  pollIntervalMs: 1,
  scheduleEveryMs: 0,
  idgen: () => crypto.randomUUID(),
});
await secretary.start();
assert.equal(await secretary.step(), "turn");
const events = await state.events(persona, 0);
assert.equal(events.filter((e) => e.kind === "note").length, 1);
assert.equal(
  events.find((e) => e.kind === "note")?.payload.text,
  "synthetic transport note",
);
const outbox = await state.outbox(persona, 0);
const last = outbox.at(-1);
assert.ok(last);
assert.equal((last.payload.output as { text: string }).text, "done");
await secretary.stop();
console.log(
  "Core -> Go -> synthetic upstream: two model rounds, one durable note, opaque continuation, no token binding",
);
