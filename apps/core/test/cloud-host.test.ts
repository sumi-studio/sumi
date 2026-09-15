import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import {
  NoSelectionProvider,
  providerFromEnv,
  SelectedModelProvider,
} from "../src/host/provider-env.ts";
import worker, { SecretaryObject } from "../src/host/workerd.ts";
import { ModelError } from "../src/provider.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000003";
const WAKE = "w".repeat(40);
const RUNTIME = "r".repeat(40);
const PUBLIC = "https://sumi-core-alpha.example.workers.dev";

function entryEnv(extra: Record<string, unknown> = {}) {
  const reached: string[] = [];
  const env = {
    SUMI_STATE_URL: "http://state.invalid:8080",
    SECRETARY: {
      idFromName: (n: string) => n,
      get: (id: unknown) => ({
        fetch: async () => {
          reached.push(String(id));
          return Response.json({ ok: true });
        },
      }),
    },
    ...extra,
  };
  return { env, reached };
}

const post = (url: string, token?: string) =>
  new Request(url, {
    method: "POST",
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  });

test("wake requires the configured bearer before any Durable Object is reached", async () => {
  const { env, reached } = entryEnv({ SUMI_CORE_WAKE_TOKEN: WAKE });
  const url = `${PUBLIC}/personas/${PERSONA}/wake`;
  assert.equal((await worker.fetch(post(url), env as never)).status, 401);
  assert.equal(
    (await worker.fetch(post(url, `${WAKE}x`), env as never)).status,
    401,
  );
  assert.equal(reached.length, 0);
  assert.equal((await worker.fetch(post(url, WAKE), env as never)).status, 200);
  assert.deepEqual(reached, [PERSONA]);
  // A malformed persona id never names a Durable Object.
  const bad = await worker.fetch(
    post(`${PUBLIC}/personas/not-a-persona/wake`, WAKE),
    env as never,
  );
  assert.equal(bad.status, 400);
  assert.equal(reached.length, 1);
  // Liveness stays public; state reachability does not.
  assert.equal(
    (await worker.fetch(new Request(`${PUBLIC}/health`), env as never)).status,
    200,
  );
  assert.equal(
    (await worker.fetch(new Request(`${PUBLIC}/health/state`), env as never))
      .status,
    401,
  );
});

test("without a wake token, wake is refused unless the dev flag meets a loopback host", async () => {
  const { env, reached } = entryEnv();
  assert.equal(
    (
      await worker.fetch(
        post(`${PUBLIC}/personas/${PERSONA}/wake`),
        env as never,
      )
    ).status,
    503,
  );
  const dev = entryEnv({ SUMI_CORE_WAKE_OPEN: "loopback-dev" });
  assert.equal(
    (
      await worker.fetch(
        post(`${PUBLIC}/personas/${PERSONA}/wake`),
        dev.env as never,
      )
    ).status,
    503,
    "the dev flag must not open a public hostname",
  );
  assert.equal(
    (
      await worker.fetch(
        post(`http://127.0.0.1:8787/personas/${PERSONA}/wake`),
        dev.env as never,
      )
    ).status,
    200,
  );
  assert.equal(reached.length, 0);
  assert.equal(dev.reached.length, 1);
});

test("/health/state probes through the SUMI_STATE binding and reports an outage as 503", async () => {
  const seen: string[] = [];
  let up = true;
  const { env } = entryEnv({
    SUMI_CORE_WAKE_TOKEN: WAKE,
    SUMI_STATE: {
      fetch: async (input: string) => {
        seen.push(input);
        if (!up) throw new Error("tunnel unavailable");
        return Response.json({ status: "ok" });
      },
    },
  });
  const probe = () =>
    worker.fetch(
      new Request(`${PUBLIC}/health/state`, {
        headers: { Authorization: `Bearer ${WAKE}` },
      }),
      env as never,
    );
  const ok = await probe();
  assert.equal(ok.status, 200);
  assert.equal(((await ok.json()) as { via: string }).via, "binding");
  up = false;
  const down = await probe();
  assert.equal(down.status, 503);
  assert.equal(((await down.json()) as { ok: boolean }).ok, false);
  assert.deepEqual(seen, [
    "http://state.invalid:8080/health",
    "http://state.invalid:8080/health",
  ]);
});

function doCtx() {
  const data = new Map<string, unknown>();
  let alarmAt: number | null = null;
  const waits: Promise<unknown>[] = [];
  return {
    waits,
    storage: {
      get: async (k: string) => data.get(k),
      put: async (k: string, v: unknown) => void data.set(k, v),
      setAlarm: async (when: number) => {
        alarmAt = when;
      },
      getAlarm: async () => alarmAt,
    },
    waitUntil: (p: Promise<unknown>) => void waits.push(p),
  };
}

async function firstStateCall(extra: Record<string, unknown>) {
  const calls: { url: string; auth: string | null }[] = [];
  const ctx = doCtx();
  const obj = new SecretaryObject(
    ctx as never,
    {
      SUMI_STATE_URL: "http://state.invalid:8080",
      SUMI_MODEL_PROVIDER: "none",
      SECRETARY: {} as never,
      SUMI_STATE: {
        fetch: async (input: string, init?: RequestInit) => {
          calls.push({
            url: input,
            auth: new Headers(init?.headers).get("Authorization"),
          });
          // A definite refusal ends the drain at once.
          return Response.json({ error: "unauthorized" }, { status: 401 });
        },
      },
      ...extra,
    } as never,
  );
  const res = await obj.fetch(
    new Request(`https://do.internal/personas/${PERSONA}/wake`, {
      method: "POST",
    }),
  );
  assert.equal(res.status, 200);
  await Promise.all(ctx.waits.splice(0));
  const first = calls[0];
  assert.ok(first, "the drain made no state call");
  return first;
}

test("secretary state calls travel through the binding with the runtime credential", async () => {
  const call = await firstStateCall({ SUMI_CORE_RUNTIME_TOKEN: RUNTIME });
  assert.equal(
    call.url,
    `http://state.invalid:8080/internal/core/personas/${PERSONA}/writer/acquire`,
  );
  assert.equal(call.auth, `Bearer ${RUNTIME}`);
});

test("a persona-specific token binding still takes precedence over the runtime credential", async () => {
  const call = await firstStateCall({
    SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
    [`SUMI_PERSONA_TOKEN_${PERSONA.replaceAll("-", "_")}`]: "core_persona",
  });
  assert.equal(call.auth, "Bearer core_persona");
});

test("SUMI_MODEL_PROVIDER=none: an unselected persona is unavailable, never answered", async () => {
  const fallback = providerFromEnv((n) =>
    n === "SUMI_MODEL_PROVIDER" ? "none" : undefined,
  );
  assert.ok(fallback instanceof NoSelectionProvider);
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.setModelBinding(PERSONA, { selection: "unset" } as never);
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback,
    timeoutMs: 1_000,
  });
  const isUnavailable = (e: unknown) =>
    e instanceof ModelError &&
    e.unavailable === true &&
    e.retryable === false &&
    /no model connection is selected/.test(e.message);
  await assert.rejects(provider.probe(), isUnavailable);
  await assert.rejects(async () => {
    for await (const _ of provider.stream({ messages: [] } as never)) {
      // unreachable
    }
  }, isUnavailable);
  await assert.rejects(async () => {
    for await (const _ of fallback.stream({ messages: [] } as never)) {
      // unreachable
    }
  }, isUnavailable);
});
