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
  // The check route sits behind the same gate.
  assert.equal(
    (
      await worker.fetch(post(`${PUBLIC}/personas/${PERSONA}/check`), env as never)
    ).status,
    401,
  );
  assert.equal(
    (
      await worker.fetch(
        post(`${PUBLIC}/personas/not-a-persona/check`, WAKE),
        env as never,
      )
    ).status,
    400,
  );
  assert.equal(reached.length, 1, "check refusals never reach a DO");
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
          // A definite refusal fails the wake's startup gate at once.
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
  assert.equal(res.status, 503);
  assert.equal(
    ((await res.json()) as { reason?: string }).reason,
    "unauthorized",
  );
  await Promise.all(ctx.waits.splice(0));
  const first = calls[0];
  assert.ok(first, "the startup gate made no state call");
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

/**
 * Enough of the state service for a DO startup + drain to idle: lease
 * acquire/renew/release, recover, dispatch, loadTurn. `authorize` decides
 * whether the presented bearer is accepted — a wrong installed secret is
 * the reviewed failure shape.
 */
function stateStub(opts: { accept: (auth: string | null) => boolean }) {
  const calls: { url: string; auth: string | null }[] = [];
  const fetcher = async (input: string, init?: RequestInit) => {
    const url = String(input);
    const auth = new Headers(init?.headers).get("Authorization");
    calls.push({ url, auth });
    if (!opts.accept(auth))
      return Response.json({ error: "unauthorized" }, { status: 401 });
    if (url.endsWith("/state"))
      return Response.json({ persona: { persona_id: PERSONA } });
    if (url.endsWith("/writer/acquire") || url.endsWith("/writer/renew"))
      return Response.json({
        persona_id: PERSONA,
        holder_id: "workerd-test",
        generation: 1,
        acquired_at: new Date().toISOString(),
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      });
    if (url.endsWith("/recover"))
      return Response.json({ interrupted_turns: [], requeued_inputs: [] });
    if (url.endsWith("/schedules/dispatch"))
      return Response.json({ fired: [] });
    if (url.endsWith("/turns/load"))
      return Response.json({ turn: null, input: null });
    if (url.endsWith("/writer/release"))
      return Response.json({ released: true });
    return Response.json({});
  };
  return { calls, fetcher };
}

const checkReq = (p = PERSONA) =>
  new Request(`https://do.internal/personas/${p}/check`, { method: "POST" });

test("check: the DO authenticates to a persona-scoped route through the binding", async () => {
  const stub = stateStub({ accept: (a) => a === `Bearer ${RUNTIME}` });
  const ctx = doCtx();
  const obj = new SecretaryObject(
    ctx as never,
    {
      SUMI_STATE_URL: "http://state.invalid:8080",
      SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
      SUMI_STATE: { fetch: stub.fetcher },
      SECRETARY: {} as never,
    } as never,
  );
  const res = await obj.fetch(checkReq());
  assert.equal(res.status, 200);
  const body = (await res.json()) as { ok: boolean; via: string };
  assert.deepEqual({ ok: body.ok, via: body.via }, { ok: true, via: "binding" });
  assert.deepEqual(stub.calls, [
    {
      url: `http://state.invalid:8080/internal/core/personas/${PERSONA}/state`,
      auth: `Bearer ${RUNTIME}`,
    },
  ]);
  // No alarm, no stored persona: a check starts nothing.
  assert.equal(ctx.waits.length, 0);
});

test("check: a wrong installed secret surfaces as 503 unauthorized", async () => {
  const stub = stateStub({ accept: (a) => a === "Bearer the-right-one" });
  const ctx = doCtx();
  const obj = new SecretaryObject(
    ctx as never,
    {
      SUMI_STATE_URL: "http://state.invalid:8080",
      SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
      SUMI_STATE: { fetch: stub.fetcher },
      SECRETARY: {} as never,
    } as never,
  );
  const res = await obj.fetch(checkReq());
  assert.equal(res.status, 503);
  const body = (await res.json()) as { reason: string; state_status: number };
  assert.equal(body.reason, "unauthorized");
  assert.equal(body.state_status, 401);
});

test("check: no credential at all is 503 no_credential, without a state call", async () => {
  const stub = stateStub({ accept: () => true });
  const obj = new SecretaryObject(
    doCtx() as never,
    {
      SUMI_STATE_URL: "http://state.invalid:8080",
      SUMI_STATE: { fetch: stub.fetcher },
      SECRETARY: {} as never,
    } as never,
  );
  const res = await obj.fetch(checkReq());
  assert.equal(res.status, 503);
  assert.equal(((await res.json()) as { reason: string }).reason, "no_credential");
  assert.equal(stub.calls.length, 0);
});

test("wake answers 503 while the runtime credential fails and 200 once it works", async () => {
  let accept = false;
  const stub = stateStub({ accept: () => accept });
  const ctx = doCtx();
  const obj = new SecretaryObject(
    ctx as never,
    {
      SUMI_STATE_URL: "http://state.invalid:8080",
      SUMI_CORE_RUNTIME_TOKEN: RUNTIME,
      SUMI_STATE: { fetch: stub.fetcher },
      SECRETARY: {} as never,
    } as never,
  );
  const wake = () =>
    obj.fetch(
      new Request(`https://do.internal/personas/${PERSONA}/wake`, {
        method: "POST",
      }),
    );
  const refused = await wake();
  assert.equal(refused.status, 503);
  assert.equal(
    ((await refused.json()) as { reason: string }).reason,
    "unauthorized",
  );
  assert.notEqual(
    await ctx.storage.getAlarm(),
    null,
    "a failed wake still arms the heartbeat so the DO retries",
  );
  // The credential now authenticates: the same DO starts and drains.
  accept = true;
  const ok = await wake();
  assert.equal(ok.status, 200);
  assert.equal(((await ok.json()) as { ok: boolean }).ok, true);
  await Promise.all(ctx.waits.splice(0));
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
