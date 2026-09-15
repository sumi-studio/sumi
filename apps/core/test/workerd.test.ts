import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { SelectedModelProvider } from "../src/host/provider-env.ts";
import {
  MissingPersonaTokenError,
  SecretaryObject,
} from "../src/host/workerd.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { MockProvider } from "../src/providers/mock.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { type StateClient, StateError } from "../src/state-client.ts";

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
  private readonly cfgOver: Partial<SecretaryConfig>;
  constructor(
    ctx: ConstructorParameters<typeof SecretaryObject>[0],
    env: ConstructorParameters<typeof SecretaryObject>[1],
    state: StateClient,
    provider: ModelProvider = new MockProvider(),
    cfgOver: Partial<SecretaryConfig> = {},
  ) {
    super(ctx, env);
    this.state = state;
    this.provider = provider;
    this.cfgOver = cfgOver;
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
      memoryPreparationTimeoutMs: this.memoryPreparationTimeoutMs(),
      idgen: () => crypto.randomUUID(),
      ...this.cfgOver,
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
  assert.equal(((await r2.json()) as { coalesced?: boolean }).coalesced, true);
  await settle(ctx);

  // The input completed exactly once — no interrupted turn, no stranded
  // claimed input waiting for a third wake.
  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1);
  assert.equal(
    [...state.turns.values()].filter((t) => t.status === "interrupted").length,
    0,
  );
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-dup")!.status,
    "done",
  );
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

class MissingTokenObject extends TestObject {
  protected override newSecretary(personaId: string): Secretary {
    throw new MissingPersonaTokenError(personaId);
  }
}

test("missing persona token re-arms on the dormant cadence and recovers (F5)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = {
    ...fakeEnv,
    SUMI_HEARTBEAT_MS: "60000",
    SUMI_DORMANT_REARM_MS: "5000",
  };
  // Activation succeeds while the token binding exists.
  const active = new TestObject(ctx as never, env as never, state);
  await active.fetch(wakeReq());
  await settle(ctx);
  assert.ok(ctx.alarmAt() !== null, "heartbeat armed");

  // Eviction with the binding now gone: the alarm must not die silently —
  // it re-arms on the long dormant cadence instead of the heartbeat.
  const dormant = new MissingTokenObject(ctx as never, env as never, state);
  const t0 = Date.now();
  await dormant.alarm();
  const dormantIn = ctx.alarmAt()! - t0;
  assert.ok(
    dormantIn > 1_000 && dormantIn <= 5_500,
    `dormant re-arm (~5s), not heartbeat/disarm: ${dormantIn}ms`,
  );
  // A second dormant alarm re-arms again — eventual recovery is durable,
  // not a one-shot.
  await dormant.alarm();
  assert.ok(ctx.alarmAt()! > Date.now());

  // The binding returns: the dormant alarm drains normally again.
  const healed = new TestObject(ctx as never, env as never, state);
  state.addInput(PERSONA, "in-healed", "hello after provisioning");
  await healed.alarm();
  await settle(ctx);
  const out = await state.outbox(PERSONA, 0);
  assert.ok(
    out.some((o) => o.payload.input_id === "in-healed"),
    "provisioned persona drains on the dormant alarm",
  );
  const hb = ctx.alarmAt()! - Date.now();
  assert.ok(hb > 30_000, "back on the heartbeat cadence after recovery");
});

/** ~11k estimated tokens per message: each exchange clears the 10k seal. */
const PAD = "x".repeat(44 * 1024);

/**
 * Turns answer at once. A memory branch answers after `branchDelayMs`, or
 * never (ignoring cancellation) when `branchHangs` is set.
 */
class BranchProvider implements ModelProvider {
  readonly name = "branch-scripted";
  branchRequests = 0;
  branchDelayMs = 0;
  branchHangs = false;
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    if (!req.turnId.startsWith("memory-l1-")) {
      yield { type: "text", delta: "ok" };
      yield { type: "done", usage: { finish_reason: "stop" } };
      return;
    }
    this.branchRequests++;
    if (this.branchHangs) await new Promise(() => {});
    await sleep(this.branchDelayMs);
    yield { type: "text", delta: "organized memory of the first exchange" };
    yield { type: "done", usage: { finish_reason: "stop" } };
  }
}

/** Two padded exchanges through fetch wakes: chunk 1 is sealable after. */
async function twoExchanges(
  state: FakeState,
  obj: TestObject,
  ctx: ReturnType<typeof fakeCtx>,
) {
  state.addInput(PERSONA, "pad-1", `first ${PAD}`);
  state.addInput(PERSONA, "pad-2", `second ${PAD}`);
  await obj.fetch(wakeReq());
  await settle(ctx);
  assert.equal((await state.outbox(PERSONA, 0)).length, 2);
}

test("fetch drain never starts memory preparation and arms the alarm to start it", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  const ctx = fakeCtx();
  const obj = new TestObject(ctx as never, fakeEnv as never, state, provider);
  await twoExchanges(state, obj, ctx);
  // A second fetch drain seals chunk 1 but must not claim it.
  await obj.fetch(wakeReq());
  await settle(ctx);
  const c = state.memoryChunks[0];
  assert.ok(c, "chunk 1 sealed");
  assert.equal(c.status, "sealed");
  assert.equal(c.interruptions, 0);
  assert.equal(provider.branchRequests, 0, "no model call from a fetch drain");
  const armedIn = ctx.alarmAt()! - Date.now();
  assert.ok(armedIn <= 1_500, `alarm armed soon for preparation: ${armedIn}ms`);
});

test("alarm drain keeps a branch alive past the turn budget and shelves its result", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  provider.branchDelayMs = 1_500;
  const ctx = fakeCtx();
  const env = {
    ...fakeEnv,
    SUMI_DRAIN_TURN_BUDGET_MS: "300",
    SUMI_ALARM_DRAIN_LIFETIME_MS: "20000",
    SUMI_MEMORY_PREPARATION_TIMEOUT_MS: "5000",
  };
  const obj = new TestObject(ctx as never, env as never, state, provider);
  await twoExchanges(state, obj, ctx);

  const t0 = performance.now(); // monotonic — the host clock may step
  await obj.alarm();
  const took = performance.now() - t0;
  const c = state.memoryChunks[0]!;
  assert.equal(c.status, "prepared", JSON.stringify(c));
  assert.equal(c.attempts, 0);
  assert.equal(c.interruptions, 0);
  assert.equal(provider.branchRequests, 1);
  assert.ok(
    took >= 1_500,
    `the alarm held the branch to completion: ${took}ms`,
  );
  assert.equal(
    state.leases.get(PERSONA)!.expires_at < new Date().toISOString(),
    true,
    "lease released after the drain",
  );
});

test("alarm drain records a hanging branch as a retryable timeout and re-arms for the retry", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  provider.branchHangs = true;
  const ctx = fakeCtx();
  const env = {
    ...fakeEnv,
    // One attempt per drain, deterministically: the drain starts a
    // branch only when timeout+margin still fits the remaining lifetime
    // (margin = lifetime/10), and a recorded timeout reshelves with a
    // ~200ms backoff. 2000ms lifetime leaves no room for a second
    // attempt — after the ~1000ms timeout, startMemory can never hold
    // again — so attempts===1 is a real guarantee here, not a timing
    // accident (a 3000ms lifetime legitimately admits a second attempt
    // at ~1.2s, which is valid product behavior, not a defect).
    SUMI_ALARM_DRAIN_LIFETIME_MS: "2000",
    SUMI_MEMORY_PREPARATION_TIMEOUT_MS: "1000",
  };
  const obj = new TestObject(ctx as never, env as never, state, provider);
  await twoExchanges(state, obj, ctx);

  await obj.alarm();
  const c = state.memoryChunks[0]!;
  assert.equal(c.status, "sealed", JSON.stringify(c));
  assert.equal(c.attempts, 1, "a timeout is a recorded failure");
  assert.equal(c.interruptions, 0);
  assert.match(c.last_error ?? "", /did not finish within 1000ms/);
  const armedIn = ctx.alarmAt()! - Date.now();
  assert.ok(
    armedIn > 0 && armedIn <= 1_500,
    `re-armed for the retry, not the 30s heartbeat: ${armedIn}ms`,
  );

  // The retry the alarm was armed for: once the reshelve backoff has
  // passed and the provider stops hanging, the next alarm claims the
  // same chunk and completes it — attempts stays at the one recorded
  // failure (success does not spend attempts).
  provider.branchHangs = false;
  await sleep(1_400); // past not_before (~timeout + 200ms backoff)
  await obj.alarm();
  const retried = state.memoryChunks[0]!;
  assert.equal(retried.status, "prepared", JSON.stringify(retried));
  assert.equal(retried.attempts, 1);
  assert.equal(retried.interruptions, 0);
  assert.equal(provider.branchRequests, 2, "the retry ran exactly once");
});

/**
 * An unbound selection with pending memory must not keep the DO on the 1s
 * memory-wake floor: the branch pauses and the alarm rests on the shelf
 * cadence until a usable binding exists, then the same chunk proceeds.
 */
test("unbound binding shelves memory work; a repaired binding resumes it", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  let bindingLookups = 0;
  const realBinding = state.modelBinding.bind(state);
  state.modelBinding = (p) => {
    bindingLookups++;
    return realBinding(p);
  };
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback: new MockProvider(),
    timeoutMs: 5_000,
  });
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  const obj = new TestObject(ctx as never, env as never, state, provider, {
    memoryUnavailablePauseMs: 2_000,
  });

  // Bound while the exchanges land and the chunk seals.
  await twoExchanges(state, obj, ctx);
  await obj.fetch(wakeReq());
  await settle(ctx);
  const c = state.memoryChunks[0]!;
  assert.equal(c.status, "sealed");

  // Now the destination-window state: the selection cannot produce a call.
  state.setModelBinding(PERSONA, {
    selection: "needs_rebinding",
    intent: { kind: "api", model: "m" },
    reason: "carried model intent needs a destination selection",
  } as never);

  const probesBefore = bindingLookups;
  const alarmStart = Date.now();
  await obj.alarm();
  assert.equal(bindingLookups, probesBefore + 1, "one probe, not a claim");
  const after = state.memoryChunks[0]!;
  assert.equal(after.status, "sealed", "the pause records no verdict");
  assert.equal(after.attempts, 0);
  assert.equal(after.interruptions, 0);
  // Measured from before the drain: the alarm fired at start+shelf, not
  // the 1s memory-wake floor.
  const armedIn = ctx.alarmAt()! - alarmStart;
  assert.ok(
    armedIn > 1_500,
    `unbound memory re-arms on the shelf cadence, not the 1s floor: ${armedIn}ms`,
  );

  // A second alarm inside the shelf runs an ordinary drain — no probe,
  // no claim — and ordinary work still wakes promptly.
  await obj.alarm();
  assert.equal(bindingLookups, probesBefore + 1, "shelved: no re-probe");
  assert.equal(state.memoryChunks[0]!.status, "sealed");
  state.addInput(PERSONA, "in-while-paused", "hello while unbound");
  await obj.fetch(wakeReq());
  await settle(ctx);
  assert.notEqual(
    state.inputs.find((i) => i.input_id === "in-while-paused")!.status,
    "queued",
    "the drain still processed the input (its own binding verdict is separate)",
  );

  // The human binds a usable selection; once the shelf expires the next
  // ordinary wake re-probes and the same chunk prepares.
  state.setModelBinding(PERSONA, { selection: "unset" });
  await sleep(2_100); // past the shelf — monotonic in-process deadline
  await obj.alarm();
  await settle(ctx);
  const done = state.memoryChunks[0]!;
  assert.ok(
    done.status === "prepared" || done.status === "kept",
    `the same chunk was worked, not burned: ${JSON.stringify(done)}`,
  );
  assert.equal(done.attempts, 0, "the pause never spent an attempt");
  assert.equal(done.interruptions, 0);
});

/**
 * A binding that dies between the preflight probe and the call resolves
 * the same `unavailable` error at stream time: the claimed chunk is
 * reshelved with its budgets intact and the wake rests on the shelf —
 * not paced to the reshelve's 200ms.
 */
test("binding dying mid-call reshelves and shelves the wake", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const realBinding = state.modelBinding.bind(state);
  let lookups = 0;
  const provider = new SelectedModelProvider({
    state,
    persona: PERSONA,
    fallback: new MockProvider(),
    timeoutMs: 5_000,
  });
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  const obj = new TestObject(ctx as never, env as never, state, provider, {
    memoryUnavailablePauseMs: 2_000,
  });
  await twoExchanges(state, obj, ctx);
  await obj.fetch(wakeReq());
  await settle(ctx);
  assert.equal(state.memoryChunks[0]!.status, "sealed");

  // Arm the dying binding only now: the probe sees it usable, the stream's
  // own resolve then gets the outage — the call-time race.
  state.modelBinding = (p) => {
    lookups++;
    if (lookups === 1) return realBinding(p);
    return Promise.reject(new StateError(503, "state service restarting"));
  };
  const alarmStart = Date.now();
  await obj.alarm();
  const c = state.memoryChunks[0]!;
  assert.equal(c.status, "sealed", "reshelved, not failed");
  assert.equal(c.attempts, 0, "the mid-call outage spent no attempt");
  assert.equal(c.interruptions, 0);
  assert.match(c.last_error ?? "", /selection lookup failed/);
  const armedIn = ctx.alarmAt()! - alarmStart;
  assert.ok(
    armedIn > 1_500,
    `reshelve's short pacing does not re-arm the alarm: ${armedIn}ms`,
  );
});
