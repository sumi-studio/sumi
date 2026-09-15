#!/usr/bin/env node
/**
 * Probe a deployed Cloud secretary core route without sending a message.
 *
 *   node scripts/operations/cloud-core-probe.mjs \
 *     --core https://sumi-core-alpha.<subdomain>.workers.dev \
 *     --wake-token-file <file> \
 *     [--state http://100.116.25.99:8080 --runtime-token-file <file>] \
 *     [--persona <uuidv7>]
 *
 * Core checks (public Worker):
 *   health        GET /health answers 200
 *   wake-auth     POST /personas/<random>/wake without a bearer answers 401
 *                 (refused before any Durable Object exists)
 *   state-path    GET /health/state with the wake bearer answers 200 via the
 *                 SUMI_STATE binding: Cloudflare → VPC Service → tunnel → API
 *   do-runtime-auth (with --persona) POST /personas/<id>/check with the wake
 *                 bearer: inside the Durable Object, the Worker's installed
 *                 runtime secret must authenticate a persona-scoped state
 *                 read through the SUMI_STATE binding. This is the check
 *                 that catches a mismatched SUMI_CORE_RUNTIME_TOKEN, which
 *                 health/state-path cannot see (unauthenticated route).
 * API checks (with --state; run where that address is reachable):
 *   runtime-scope the runtime credential reads a persona's state (needs
 *                 --persona) and is refused on persona creation (401)
 * Optional wake (with --persona): POST that persona's wake with the bearer;
 * it waits for the DO to start (lease + recovery, not a turn) and answers
 * 503 if the runtime cannot start.
 *
 * Tokens are read from files and never printed. Exit 1 on any failed check.
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";

const { values: a } = parseArgs({
  options: {
    core: { type: "string" },
    "wake-token-file": { type: "string" },
    state: { type: "string" },
    "runtime-token-file": { type: "string" },
    persona: { type: "string" },
  },
});
if (!a.core || !a["wake-token-file"]) {
  console.error(
    "usage: --core URL --wake-token-file FILE [--state URL --runtime-token-file FILE] [--persona UUIDV7]",
  );
  process.exit(2);
}
const token = (file) => readFileSync(file, "utf8").trim();
const core = a.core.replace(/\/+$/, "");
const wake = token(a["wake-token-file"]);
const results = [];
async function check(name, fn) {
  const started = Date.now();
  try {
    const detail = await fn();
    results.push({
      check: name,
      ok: true,
      ms: Date.now() - started,
      ...detail,
    });
  } catch (e) {
    results.push({
      check: name,
      ok: false,
      ms: Date.now() - started,
      error: e instanceof Error ? e.message : String(e),
    });
  }
  console.log(JSON.stringify(results.at(-1)));
}
const call = async (url, init = {}) => {
  const res = await fetch(url, {
    redirect: "manual",
    signal: AbortSignal.timeout(15_000),
    ...init,
  });
  const text = await res.text();
  let body = null;
  try {
    body = JSON.parse(text);
  } catch {}
  return { status: res.status, body };
};
const expect = (r, status, what) => {
  if (r.status !== status)
    throw new Error(
      `${what}: expected ${status}, got ${r.status} ${JSON.stringify(r.body)}`,
    );
  return { status: r.status, body: r.body };
};
const uuidv7 = () => {
  const now = Date.now().toString(16).padStart(12, "0");
  const r = randomUUID().replaceAll("-", "");
  return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
};

await check("health", async () =>
  expect(await call(`${core}/health`), 200, "GET /health"),
);
await check("wake-auth", async () =>
  expect(
    await call(`${core}/personas/${uuidv7()}/wake`, { method: "POST" }),
    401,
    "unauthenticated wake",
  ),
);
await check("state-path", async () => {
  const r = expect(
    await call(`${core}/health/state`, {
      headers: { Authorization: `Bearer ${wake}` },
    }),
    200,
    "GET /health/state",
  );
  if (r.body?.via !== "binding")
    throw new Error(`state reached via ${r.body?.via}, not the binding`);
  return r;
});
if (a.state) {
  if (!a["runtime-token-file"]) {
    console.error("--state needs --runtime-token-file");
    process.exit(2);
  }
  const state = a.state.replace(/\/+$/, "");
  const runtime = token(a["runtime-token-file"]);
  const auth = {
    Authorization: `Bearer ${runtime}`,
    "Content-Type": "application/json",
  };
  if (a.persona) {
    await check("runtime-scope-read", async () => {
      const r = await call(
        `${state}/internal/core/personas/${a.persona}/state`,
        { headers: auth },
      );
      return expect(
        { status: r.status, body: { persona: r.body?.persona?.persona_id } },
        200,
        "runtime read",
      );
    });
  }
  await check("runtime-scope-admin-refused", async () =>
    expect(
      await call(`${state}/internal/core/personas`, {
        method: "POST",
        headers: auth,
        body: JSON.stringify({ persona_id: uuidv7() }),
      }),
      401,
      "runtime credential on persona creation",
    ),
  );
}
if (a.persona) {
  await check("do-runtime-auth", async () => {
    const r = expect(
      await call(`${core}/personas/${a.persona}/check`, {
        method: "POST",
        headers: { Authorization: `Bearer ${wake}` },
      }),
      200,
      "DO authenticated state read",
    );
    if (r.body?.via !== "binding")
      throw new Error(
        `DO reached state via ${r.body?.via ?? "?"}, not the binding`,
      );
    return { status: r.status, body: { ok: r.body?.ok, via: r.body?.via } };
  });
  await check("wake-persona", async () =>
    expect(
      await call(`${core}/personas/${a.persona}/wake`, {
        method: "POST",
        headers: { Authorization: `Bearer ${wake}` },
      }),
      200,
      "authenticated wake",
    ),
  );
}
const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify({
    summary: failed.length ? "FAIL" : "PASS",
    failed: failed.map((r) => r.check),
  }),
);
process.exit(failed.length ? 1 : 0);
