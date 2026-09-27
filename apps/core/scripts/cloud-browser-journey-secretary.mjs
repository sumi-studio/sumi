// Scripted secretary for the Cloud browser journey (apps/web/e2e/
// cloud-browser.spec.ts). The real Secretary runs against the real API with
// a deterministic, harness-driven model provider: the harness writes one JSON
// command per line on stdin ({id, call, args, wait}); the child submits an
// input, the provider emits exactly that durable Core tool call, and (with
// `wait`) keeps polling job.status in the same turn until the job ends. The
// tool result is printed as one JSON line {id, result}. No model is involved.
//   CB_API_URL, CB_PERSONA, CB_PERSONA_TOKEN, CB_HUMAN
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { Secretary } from "../src/secretary.ts";
import { HttpStateClient } from "../src/state-client.ts";

const api = process.env.CB_API_URL;
const persona = process.env.CB_PERSONA;
const token = process.env.CB_PERSONA_TOKEN;
const human = process.env.CB_HUMAN;
const TERMINAL = new Set(["done", "failed", "cancelled", "lost"]);

const queue = [];
let current;
let seq = 0;
const processed = new Set();
const out = (value) => process.stdout.write(`${JSON.stringify(value)}\n`);

async function submit(text) {
  const response = await fetch(`${api}/internal/core/personas/${persona}/inputs`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({
      input_id: `cloud-browser-journey-${crypto.randomUUID()}`,
      kind: "message",
      payload: { text },
      actor_kind: "human",
      actor_id: human,
      source_surface: "e2e",
    }),
  });
  if (response.status !== 201) throw new Error(`submit input ${response.status}`);
}

const provider = {
  name: "cloud-browser-journey",
  async *stream(req) {
    let next;
    const latest = req.messages.findLast((m) => m.role === "tool");
    const key = latest ? `${req.turnId}:${latest.toolCallId}` : "";
    if (current && latest && !processed.has(key) && current.issued) {
      processed.add(key);
      let value;
      try {
        value = JSON.parse(latest.content);
      } catch {
        value = { raw: latest.content };
      }
      const job = value?.job;
      if (current.wait && job?.job_id && !TERMINAL.has(job.status) && Date.now() < current.deadline) {
        await delay(250);
        next = { name: "job.status", arguments: { job_id: job.job_id } };
      } else {
        out({ id: current.id, result: value });
        current = undefined;
      }
    }
    if (!next && current && !current.issued) {
      current.issued = true;
      if (!req.tools.some((t) => t.name === current.call)) {
        out({ id: current.id, error: `tool ${current.call} not offered` });
        current = undefined;
      } else next = { name: current.call, arguments: current.args ?? {} };
    }
    if (next) yield { type: "tool_call", call: { id: `cb-${seq++}`, route: "normal", ...next } };
    else yield { type: "text", delta: "ok" };
    yield { type: "done", usage: {} };
  },
};

const secretary = new Secretary({
  personaId: persona,
  holderId: "cloud-browser-journey",
  state: new HttpStateClient(api, token),
  provider,
  leaseTtlMs: 30_000,
  renewEveryMs: 5_000,
  contextLimit: 60,
  maxToolRounds: 400,
  pollIntervalMs: 50,
  scheduleEveryMs: 1_000_000,
  idgen: () => crypto.randomUUID(),
});

let stopping = false;
createInterface({ input: process.stdin }).on("line", (line) => {
  if (!line.trim()) return;
  const command = JSON.parse(line);
  if (command.stop) {
    stopping = true;
    return;
  }
  queue.push(command);
});

await secretary.start();
out({ ready: true });
try {
  while (!stopping) {
    if (!current && queue.length) {
      current = { ...queue.shift(), issued: false };
      current.deadline = Date.now() + (current.timeoutMs ?? 60_000);
      await submit(`journey step ${current.id}`);
    }
    await secretary.step();
    await delay(30);
  }
} finally {
  await secretary.stop();
}
