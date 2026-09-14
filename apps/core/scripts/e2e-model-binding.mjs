#!/usr/bin/env node
/**
 * E2E: selected-connection model binding, end to end.
 * REAL Go state service + REAL PostgreSQL + real Node core process +
 * a deterministic stub OpenAI Responses provider on loopback.
 *
 * Covers: human + connection seeded through state-dev (credential sealed,
 * extra headers preserved) → human selects the connection → persona turn
 * resolves the binding through the real Go endpoint → the core calls the
 * stub with the connection's API key AND extra headers → a normal-route
 * journal.note tool call executes and its result goes back as a
 * function_call_output item → final text reaches the outbox.
 *
 * Requires SUMI_TEST_DB_URL (or SUMI_DB_URL). Uses SUMI_STATE_DEV_BIN if
 * set, else builds ./cmd/state-dev with go.
 *
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:5432/postgres?sslmode=disable \
 *     node scripts/e2e-model-binding.mjs
 */
import { spawn, spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
if (!DB_URL) {
  console.error(
    "e2e: SUMI_TEST_DB_URL (or SUMI_DB_URL) required — real PostgreSQL",
  );
  process.exit(2);
}
const API_DIR = resolve(import.meta.dirname, "../../api");
const CORE_DIR = resolve(import.meta.dirname, "..");
const PORT = Number(process.env.SUMI_E2E_PORT ?? 11410 + (process.pid % 20));
const BASE = `http://127.0.0.1:${PORT}`;
const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;
const KEY = randomBytes(32).toString("base64");
const API_KEY = `e2e-stub-key-${randomUUID().replaceAll("-", "")}`;
const EXTRA_HEADERS = {
  "X-E2E-Gateway": "fixture",
  "X-E2E-Route": "model-binding",
};

function uuidv7() {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
}
const humanId = uuidv7();
const personaId = uuidv7();

function log(...a) {
  console.log("[e2e]", ...a);
}
function fail(msg) {
  console.error("[e2e] FAIL:", msg);
  process.exit(1);
}
function assert(cond, msg) {
  if (!cond) fail(msg);
}

// --- stub OpenAI Responses provider --------------------------------------
// First request: emit a normal-route journal.note tool call. Second
// request (which must carry a function_call_output item): emit final text.
const requests = [];
function sse(res, events) {
  res.writeHead(200, { "content-type": "text/event-stream" });
  for (const [type, data] of events) {
    res.write(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`);
  }
  res.end();
}
const stub = createServer((req, res) => {
  if (req.method !== "POST" || req.url !== "/responses") {
    res.writeHead(404).end();
    return;
  }
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    let parsed = null;
    try {
      parsed = JSON.parse(body);
    } catch {
      /* log raw below */
    }
    requests.push({ headers: req.headers, body: parsed });
    const n = requests.length;
    log(`stub request #${n}:`, body.slice(0, 400));
    if (!parsed) {
      res.writeHead(400).end();
      return;
    }
    const sawToolResult =
      Array.isArray(parsed.input) &&
      parsed.input.some((item) => item.type === "function_call_output");
    if (!sawToolResult) {
      sse(res, [
        ["response.created", { type: "response.created" }],
        [
          "response.output_item.added",
          {
            type: "response.output_item.added",
            item: {
              type: "function_call",
              id: "fc_e2e_1",
              call_id: "call_e2e_1",
              name: "journal_note",
            },
          },
        ],
        [
          "response.function_call_arguments.delta",
          {
            type: "response.function_call_arguments.delta",
            item_id: "fc_e2e_1",
            delta: '{"route":"normal","input":{"tex',
          },
        ],
        [
          "response.function_call_arguments.delta",
          {
            type: "response.function_call_arguments.delta",
            item_id: "fc_e2e_1",
            delta: 't":"remembered via e2e"}}',
          },
        ],
        [
          "response.output_item.done",
          {
            type: "response.output_item.done",
            item: {
              type: "function_call",
              id: "fc_e2e_1",
              call_id: "call_e2e_1",
              name: "journal_note",
              arguments:
                '{"route":"normal","input":{"text":"remembered via e2e"}}',
            },
          },
        ],
        [
          "response.completed",
          {
            type: "response.completed",
            response: {
              usage: { input_tokens: 11, output_tokens: 7, total_tokens: 18 },
            },
          },
        ],
      ]);
    } else {
      sse(res, [
        ["response.created", { type: "response.created" }],
        [
          "response.output_text.delta",
          { type: "response.output_text.delta", delta: "noted: " },
        ],
        [
          "response.output_text.delta",
          { type: "response.output_text.delta", delta: "remembered via e2e" },
        ],
        [
          "response.completed",
          {
            type: "response.completed",
            response: {
              usage: { input_tokens: 21, output_tokens: 5, total_tokens: 26 },
            },
          },
        ],
      ]);
    }
  });
});
await new Promise((r) => stub.listen(0, "127.0.0.1", r));
const STUB_URL = `http://127.0.0.1:${stub.address().port}`;
process.on("exit", () => stub.close());
log("stub Responses provider on", STUB_URL);

// --- state-dev -----------------------------------------------------------
let bin = process.env.SUMI_STATE_DEV_BIN;
if (!bin) {
  const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-"));
  bin = join(binDir, "state-dev");
  log("building state-dev…");
  const build = spawnSync(
    "go",
    ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"],
    { cwd: API_DIR, stdio: "inherit" },
  );
  if (build.status !== 0) fail("go build failed");
}

log("starting state-dev on", BASE);
const svc = spawn(bin, [], {
  env: {
    ...process.env,
    SUMI_DB_URL: DB_URL,
    SUMI_CORE_STATE_TOKEN: ADMIN,
    SUMI_MODEL_CONNECTION_KEY: KEY,
    SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
  },
  stdio: ["ignore", "pipe", "pipe"],
});
svc.stderr.on("data", (d) => process.stderr.write(`[state-dev] ${d}`));
process.on("exit", () => svc.kill("SIGKILL"));

async function req(method, path, token, body) {
  const res = await fetch(BASE + path, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* non-JSON */
  }
  return { status: res.status, json, text };
}

async function waitHealth(deadlineMs) {
  while (Date.now() < deadlineMs) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return;
    } catch {
      /* not up yet */
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  fail("state-dev did not become healthy");
}
await waitHealth(Date.now() + 15_000);

// --- seed human, connection, selection ------------------------------------
log("seeding human + openai-responses connection + selection");
let r = await req("POST", "/internal/dev/humans", ADMIN, {
  human_id: humanId,
});
assert(
  r.status === 201 || r.status === 200,
  `seed human ${r.status}: ${r.text}`,
);
r = await req("POST", "/internal/dev/model-connections", ADMIN, {
  human_id: humanId,
  name: "e2e responses stub",
  preset: "openai-responses",
  base_url: STUB_URL,
  model: "e2e-model",
  api_key: API_KEY,
  extra_headers: EXTRA_HEADERS,
});
assert(
  r.status === 201 || r.status === 200,
  `seed conn ${r.status}: ${r.text}`,
);
const connectionId = r.json.connection_id;
assert(connectionId, "seeded connection_id missing");
assert(
  !JSON.stringify(r.json ?? {}).includes("fixture"),
  "connection response leaks extra header values",
);
r = await req("POST", "/internal/dev/model-selections", ADMIN, {
  human_id: humanId,
  kind: "api",
  connection_id: connectionId,
});
assert(
  r.status === 201 || r.status === 200,
  `selection ${r.status}: ${r.text}`,
);

// --- provision persona bound to that human --------------------------------
r = await req("POST", "/internal/core/personas", ADMIN, {
  persona_id: personaId,
  human_id: humanId,
  display_name: "e2e model binding",
});
assert(r.status === 201, `createPersona ${r.status}: ${r.text}`);
const ptoken = r.json.persona_token;
assert(ptoken?.startsWith("core_"), "persona token missing");

// Binding sanity: the core-facing endpoint must carry key + headers.
r = await req("GET", `/internal/core/personas/${personaId}/model`, ptoken);
assert(r.status === 200, `model-binding ${r.status}: ${r.text}`);
const binding = r.json;
assert(
  binding?.selection === "api",
  `unexpected selection ${binding?.selection}`,
);
assert(
  binding.connection?.preset === "openai-responses",
  `unexpected preset ${binding.connection?.preset}`,
);
assert(binding.connection?.base_url === STUB_URL, "binding base_url mismatch");
assert(
  binding.api_key === API_KEY,
  "binding api_key mismatch (credential not resolved)",
);
assert(binding.credential_available === true, "credential_available not set");
assert(
  binding.connection?.extra_headers?.["X-E2E-Gateway"] === "fixture",
  "binding extra_headers missing X-E2E-Gateway",
);
log("  binding resolves with credential + extra headers");

// --- run the core turn -----------------------------------------------------
r = await req("POST", `/internal/core/personas/${personaId}/inputs`, ptoken, {
  input_id: `in-${randomUUID()}`,
  kind: "message",
  payload: { text: "remember this please" },
  actor_kind: "human",
  actor_id: "e2e",
  source_surface: "e2e",
});
assert(r.status === 201, `submitInput ${r.status}: ${r.text}`);

// The stub lives in this process — spawn (not spawnSync) so the event
// loop keeps serving it while the core runs.
const once = await new Promise((resolve) => {
  const child = spawn("node", [join(CORE_DIR, "src/host/local.ts"), "--once"], {
    env: {
      ...process.env,
      SUMI_STATE_URL: BASE,
      SUMI_PERSONA_ID: personaId,
      SUMI_PERSONA_TOKEN: ptoken,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let out = "";
  let err = "";
  child.stdout.on("data", (d) => (out += d));
  child.stderr.on("data", (d) => (err += d));
  const killer = setTimeout(() => child.kill("SIGKILL"), 90_000);
  child.on("exit", (status, signal) => {
    clearTimeout(killer);
    resolve({ status, signal, stdout: out, stderr: err });
  });
});
if (once.status !== 0) {
  console.error(once.stdout, once.stderr);
  console.error("[e2e] stub saw", requests.length, "requests");
  try {
    const evs = await req(
      "GET",
      `/internal/core/personas/${personaId}/events?after_seq=0`,
      ptoken,
    );
    console.error("[e2e] events:", JSON.stringify(evs.json, null, 1));
  } catch (e) {
    console.error("[e2e] event dump failed:", e);
  }
  fail(
    `local --once exited nonzero (status=${once.status} signal=${once.signal})`,
  );
}

// --- assertions ------------------------------------------------------------
assert(
  requests.length === 2,
  `expected 2 provider calls (tool call, then result), got ${requests.length}`,
);
for (const [i, q] of requests.entries()) {
  assert(
    q.headers.authorization === `Bearer ${API_KEY}`,
    `request ${i + 1} authorization header mismatch`,
  );
  for (const [name, value] of Object.entries(EXTRA_HEADERS)) {
    assert(
      q.headers[name.toLowerCase()] === value,
      `request ${i + 1} missing extra header ${name}`,
    );
  }
}
const second = requests[1].body;
const toolOut = second.input.find((i) => i.type === "function_call_output");
assert(toolOut, "second request lacks function_call_output");
assert(
  toolOut.call_id === "call_e2e_1",
  `function_call_output call_id ${toolOut.call_id} != call_e2e_1`,
);
assert(
  second.input.some(
    (i) => i.type === "function_call" && i.call_id === "call_e2e_1",
  ),
  "second request does not replay the original function_call",
);
log("  provider saw api key + extra headers + function_call_output replay");

const out = await req(
  "GET",
  `/internal/core/personas/${personaId}/outbox?after_seq=0`,
  ptoken,
);
assert(
  out.json.outbox.length === 1,
  `expected 1 outbox reply, got ${out.json.outbox.length}`,
);
assert(
  out.json.outbox[0].kind === "turn_completed",
  `unexpected outbox kind ${out.json.outbox[0].kind}`,
);
assert(
  out.json.outbox[0].payload.output.text === "noted: remembered via e2e",
  `reply text mismatch: ${out.json.outbox[0].payload.output.text}`,
);
log("  outbox reply:", out.json.outbox[0].payload.output.text);
log("PASS");
process.exit(0);
