import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SecretaryObject } from "../src/host/workerd.ts";
import type { ModelProvider } from "../src/provider.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { Secretary } from "../src/secretary.ts";
import type { StateClient } from "../src/state-client.ts";

const PERSONA = "01930e00-0000-7000-8000-000000000002";

/** In-memory DO ctx: storage map + alarm slot + waitUntil collection. */
function fakeCtx() {
  const data = new Map<string, unknown>();
  let alarmAt: number | null = null;
  const waits: Promise<unknown>[] = [];
  const ctx = {
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
    alarmAt: () => alarmAt,
  };
  return ctx;
}

const fakeEnv = { SUMI_STATE_URL: "http://unused", SECRETARY: {} as never };

class TestObject extends SecretaryObject {
  private readonly state: StateClient;
  private readonly provider: ModelProvider;
  constructor(
    ctx: ConstructorParameters<typeof SecretaryObject>[0],
    env: ConstructorParameters<typeof SecretaryObject>[1],
    state: StateClient,
    provider: ModelProvider = new MockProvider(),
  ) {
    super(ctx, env);
    this.state = state;
    this.provider = provider;
  }
  protected override newSecretary(personaId: string): Secretary {
    return new Secretary({
      personaId,
      holderId: `workerd-${personaId}`,
      state: this.state,
      provider: this.provider,
      leaseTtlMs: 4_000,
      renewEveryMs: 1_000,
      contextLimit: 50,
      pollIntervalMs: 0,
      scheduleEveryMs: 1,
      idgen: () => crypto.randomUUID(),
    });
  }
}

const wakeReq = (p = PERSONA) =>
  new Request(`https://do.internal/personas/${p}/wake`, { method: "POST" });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const settle = (ctx: { waits: Promise<unknown>[] }) =>
  Promise.all(ctx.waits.splice(0));

test("duplicate wake is coalesced into one serialized drain — no self-fencing (F2)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-dup", "!slow 250 hi");
  const ctx = fakeCtx();
  const obj = new TestObject(ctx as never, fakeEnv as never, state);

  const r1 = await obj.fetch(wakeReq());
  assert.equal(r1.status, 200);
  await sleep(50); // drain 1 is mid-turn now
  const r2 = await obj.fetch(wakeReq());
  assert.equal(
    ((await r2.json()) as { coalesced?: boolean }).coalesced,
    true,
  );
  await settle(ctx);

  // The input completed exactly once — no interrupted turn, no stranded
  // claimed input waiting for a third wake.
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1);
  assert.equal(
    [...state.turns.values()].filter((t) => t.status === "interrupted")
      .length,
    0,
  );
  assert.equal(state.inputs.find((i) => i.input_id === "in-dup")!.status, "done");
  assert.ok(ctx.alarmAt() !== null, "fetch armed the heartbeat alarm");
});

test("alarm drains work and re-arms; survives DO eviction via stored persona (F3)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const obj = new TestObject(ctx as never, fakeEnv as never, state);
  await obj.fetch(wakeReq());
  await settle(ctx);
  const armed = ctx.alarmAt();
  assert.ok(armed !== null);

  // Simulate eviction: a fresh DO instance over the same storage, personaId
  // forgotten. Work arriving now must still be picked up by the alarm.
  const obj2 = new TestObject(ctx as never, fakeEnv as never, state);
  state.addInput(PERSONA, "in-evicted", "hello after eviction");
  const before = ctx.alarmAt();
  await obj2.alarm();

  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1, "evicted DO's alarm still drains the input");
  assert.ok(
    ctx.alarmAt() !== null && ctx.alarmAt()! >= before!,
    "alarm re-armed for the next heartbeat",
  );
});

test("due schedule fires via alarm without any further fetch (F3)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(
    PERSONA,
    "in-sched",
    '!schedule.set {"wake_at":"+30","payload":{"text":"alarm-fired ping"}}',
  );
  const ctx = fakeCtx();
  const obj = new TestObject(ctx as never, fakeEnv as never, state);
  await obj.fetch(wakeReq());
  await settle(ctx);
  assert.equal(
    [...state.schedules.values()].length,
    1,
    "schedule.set committed",
  );

  await sleep(60); // wake_at now in the past; no fetch arrives — alarm drives
  await obj.alarm();
  const evs = await state.events(PERSONA, 0);
  const wake = evs.find(
    (e) => e.kind === "input_received" && e.payload.actor_kind === "schedule",
  );
  assert.ok(wake, "scheduled wake input was dispatched by the alarm drain");
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 2); // the schedule.set turn + the wake turn
  const reply = outbox.find((o) =>
    /alarm-fired ping/.test(JSON.stringify(o.payload)),
  );
  assert.ok(reply, "wake turn reply reached the outbox");
});

test("alarm on a never-activated DO is a no-op", async () => {
  const ctx = fakeCtx();
  const obj = new TestObject(ctx as never, fakeEnv as never, new FakeState());
  await obj.alarm(); // must not throw
  assert.equal(ctx.alarmAt(), null);
});
