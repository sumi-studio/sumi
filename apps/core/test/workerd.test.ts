import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
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
import {
  PersonaNotFoundError,
  type StateClient,
  StateError,
} from "../src/state-client.ts";

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
      deleteAlarm: async () => {
        alarmAt = null;
      },
      deleteAll: async () => data.clear(),
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
/** Deliver the alarm as the platform does: the fired alarm is consumed. */
const fire = async (
  ctx: { storage: { deleteAlarm(): Promise<void> } },
  obj: SecretaryObject,
) => {
  await ctx.storage.deleteAlarm();
  await obj.alarm();
};
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
  assert.equal(ctx.alarmAt(), null, "idle after the drain: no alarm");
});

test("an alarm drains work via the stored persona after eviction (F3)", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const obj = new TestObject(ctx as never, fakeEnv as never, state);
  await obj.fetch(wakeReq());
  await settle(ctx);

  // Simulate eviction: a fresh DO instance over the same storage, personaId
  // forgotten. An alarm (e.g. a guard left by a drain the eviction cut
  // short) still finds the persona and drains the waiting input.
  const obj2 = new TestObject(ctx as never, fakeEnv as never, state);
  state.addInput(PERSONA, "in-evicted", "hello after eviction");
  await fire(ctx, obj2);

  const outbox = await state.outbox(PERSONA, 0);
  assert.equal(outbox.length, 1, "evicted DO's alarm still drains the input");
  assert.equal(ctx.alarmAt(), null, "nothing left: no alarm");
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
  // The drain armed its own wake for the schedule (the 1s floor).
  const armed = ctx.alarmAt();
  assert.ok(armed !== null && armed - Date.now() <= 1_100, `armed: ${armed}`);

  await sleep(60); // wake_at now in the past; no fetch arrives — alarm drives
  await fire(ctx, obj);
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
  assert.equal(ctx.alarmAt(), null, "schedule fired: nothing left to wake for");
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
  assert.equal(ctx.alarmAt(), null, "idle: asleep");

  // Eviction with the binding now gone: an alarm must not die silently —
  // it re-arms on the long dormant cadence instead of the heartbeat.
  const dormant = new MissingTokenObject(ctx as never, env as never, state);
  const t0 = Date.now();
  await fire(ctx, dormant);
  const dormantIn = ctx.alarmAt()! - t0;
  assert.ok(
    dormantIn > 1_000 && dormantIn <= 5_500,
    `dormant re-arm (~5s), not heartbeat/disarm: ${dormantIn}ms`,
  );
  // A second dormant alarm re-arms again — eventual recovery is durable,
  // not a one-shot.
  await fire(ctx, dormant);
  assert.ok(ctx.alarmAt()! > Date.now());

  // The binding returns: the dormant alarm drains normally again.
  const healed = new TestObject(ctx as never, env as never, state);
  state.addInput(PERSONA, "in-healed", "hello after provisioning");
  await fire(ctx, healed);
  await settle(ctx);
  const out = await state.outbox(PERSONA, 0);
  assert.ok(
    out.some((o) => o.payload.input_id === "in-healed"),
    "provisioned persona drains on the dormant alarm",
  );
  assert.equal(ctx.alarmAt(), null, "recovered and idle: asleep again");
});

const PAD = "x".repeat(44 * 1024);
class BranchProvider implements ModelProvider {
  readonly name = "branch-scripted";
  branchRequests = 0;
  requests: ModelRequest[] = [];
  branchHangs = false;
  branchDelayMs = 0;
  async *stream(req: ModelRequest): AsyncIterable<ModelEvent> {
    this.requests.push(req);
    if (req.phase !== "memory") {
      yield { type: "text", delta: "ok" };
      yield { type: "done", usage: { finish_reason: "stop" } };
      return;
    }
    this.branchRequests++;
    if (this.branchHangs) await new Promise(() => {});
    await sleep(this.branchDelayMs);
    const paths = {
      candidate: "memory/1/candidate.md",
      source: "memory/1/source.json",
    };
    const tool = (name: string, args: Record<string, unknown>) => ({
      type: "tool_call" as const,
      call: {
        id: crypto.randomUUID(),
        route: "normal" as const,
        name,
        arguments: args,
      },
    });
    if (req.round === 0)
      yield tool("file.write", {
        path: paths.candidate,
        content_text: "organized first exchange",
        expect_version: "none",
      });
    else if (req.round === 1) {
      yield tool("file.read", { path: paths.source, len: 1048576 });
      yield tool("file.read", { path: paths.candidate });
    } else if (req.round === 2) {
      const written = req.messages
        .filter((m) => m.role === "tool")
        .map((m) => JSON.parse(m.content))
        .find((v) => v.path === paths.candidate);
      yield {
        type: "text",
        delta: JSON.stringify({
          action: "review",
          version: written.version,
          sha256: written.sha256,
        }),
      };
    } else {
      const frame = JSON.parse(
        req.messages.at(-1)!.content,
      ).memory_confirmation;
      yield {
        type: "text",
        delta: JSON.stringify({
          action: "confirm",
          version: frame.version,
          sha256: frame.sha256,
          token: frame.token,
          checks: {
            source_and_speakers: true,
            sequence_and_changes: true,
            uncertainty_and_relationship: true,
            no_new_conclusions: true,
            satisfied_with_this_version: true,
          },
        }),
      };
    }
    yield {
      type: "done",
      usage: { finish_reason: req.round < 2 ? "tool_calls" : "stop" },
    };
  }
}
async function twoExchanges(
  state: FakeState,
  obj: TestObject,
  ctx: ReturnType<typeof fakeCtx>,
) {
  state.registerEffect("file.read", () => {
    throw new Error("memory branch must not reach ordinary files");
  });
  state.registerEffect("file.write", () => {
    throw new Error("memory branch must not reach ordinary files");
  });
  state.addInput(PERSONA, "pad-1", `first ${PAD}`);
  state.addInput(PERSONA, "pad-2", `second ${PAD}`);
  await obj.fetch(wakeReq());
  await settle(ctx);
}
test("fetch saves the actual live snapshot; a reconstructed DO runs its private branch on alarm", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  const ctx = fakeCtx();
  await twoExchanges(
    state,
    new TestObject(ctx as never, fakeEnv as never, state, provider),
    ctx,
  );
  const b = state.memoryBranches.get(`${PERSONA}|1`);
  assert.ok(b, "trigger snapshot saved in fetch drain");
  assert.equal(b.chunk.status, "preparing");
  assert.equal(provider.branchRequests, 0);
  const frozen = JSON.stringify(b.snapshot);
  await fire(
    ctx,
    new TestObject(ctx as never, fakeEnv as never, state, provider),
  );
  assert.equal(state.memoryChunks[0]!.status, "prepared");
  assert.equal(provider.branchRequests, 4);
  assert.equal(JSON.stringify(b.snapshot), frozen);
  for (const req of provider.requests.filter((r) => r.phase === "memory")) {
    assert.equal(
      JSON.stringify(req.messages.slice(0, b.snapshot.messages.length)),
      JSON.stringify(b.snapshot.messages),
    );
    assert.equal(JSON.stringify(req.tools), JSON.stringify(b.snapshot.tools));
  }
});
test("a state outage and lost alarm recover the saved branch without rebuilding its prefix", async () => {
  const state = new FailingAcquireState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  await twoExchanges(
    state,
    new TestObject(ctx as never, env as never, state, provider),
    ctx,
  );
  const frozen = JSON.stringify(
    state.memoryBranches.get(`${PERSONA}|1`)!.snapshot,
  );
  state.failWith = () => new StateError(503, "temporary outage");
  await fire(ctx, new TestObject(ctx as never, env as never, state, provider));
  assert.ok(ctx.alarmAt());
  state.failWith = null;
  const resumed = new TestObject(ctx as never, env as never, state, provider);
  await resumed.fetch(wakeReq());
  await settle(ctx);
  assert.equal(provider.branchRequests, 0);
  await fire(ctx, resumed);
  assert.equal(state.memoryChunks[0]!.status, "prepared");
  assert.equal(
    JSON.stringify(state.memoryBranches.get(`${PERSONA}|1`)!.snapshot),
    frozen,
  );
});
test("alarm execution holds a reviewed multi-round branch beyond the normal turn budget", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  provider.branchDelayMs = 100;
  const ctx = fakeCtx();
  const env = {
    ...fakeEnv,
    SUMI_DRAIN_TURN_BUDGET_MS: "100",
    SUMI_ALARM_DRAIN_LIFETIME_MS: "10000",
  };
  await twoExchanges(
    state,
    new TestObject(ctx as never, env as never, state, provider),
    ctx,
  );
  await fire(ctx, new TestObject(ctx as never, env as never, state, provider));
  assert.equal(state.memoryChunks[0]!.status, "prepared");
  assert.equal(provider.branchRequests, 4);
});
test("interrupted alarm leaves durable progress and later alarm resumes the same source", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const provider = new BranchProvider();
  provider.branchHangs = true;
  const ctx = fakeCtx();
  const env = {
    ...fakeEnv,
    SUMI_MEMORY_PREPARATION_TIMEOUT_MS: "1000",
    SUMI_ALARM_DRAIN_LIFETIME_MS: "2000",
  };
  await twoExchanges(
    state,
    new TestObject(ctx as never, env as never, state, provider),
    ctx,
  );
  await fire(ctx, new TestObject(ctx as never, env as never, state, provider));
  assert.ok(state.memoryBranches.get(`${PERSONA}|1`)!.state!.in_flight);
  provider.branchHangs = false;
  await fire(
    ctx,
    new TestObject(
      ctx as never,
      { ...fakeEnv, SUMI_MEMORY_PREPARATION_TIMEOUT_MS: "5000" } as never,
      state,
      provider,
    ),
  );
  assert.equal(state.memoryChunks[0]!.status, "prepared");
});

const PERSONA_KEY_FOR_TEST = "sumi/persona_id";

/** Counts storage writes and can fail the next get/put once. */
function countingCtx() {
  const ctx = fakeCtx();
  const counts = { put: 0, get: 0 };
  const fail = { get: false, put: false };
  const { get, put } = ctx.storage;
  ctx.storage.get = async (k: string) => {
    counts.get++;
    if (fail.get) {
      fail.get = false;
      throw new Error("storage get failed");
    }
    return get(k);
  };
  ctx.storage.put = async (k: string, v: unknown) => {
    if (fail.put) {
      fail.put = false;
      throw new Error("storage put failed");
    }
    counts.put++;
    return put(k, v);
  };
  return { ctx, counts, fail };
}

test("a reconstructed instance reuses the stored persona id without rewriting it", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const { ctx, counts } = countingCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);
  assert.equal(counts.put, 1, "activation stores the persona id once");

  // Alarm on a fresh instance: the id it read drives the drain.
  state.addInput(PERSONA, "in-alarm", "after eviction");
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-alarm")!.status,
    "done",
  );

  // Wake on another fresh instance.
  state.addInput(PERSONA, "in-fetch", "wake after eviction");
  const r = await new TestObject(ctx as never, env as never, state).fetch(
    wakeReq(),
  );
  assert.equal(r.status, 200);
  await settle(ctx);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-fetch")!.status,
    "done",
  );
  assert.equal(counts.put, 1, "no rewrite after reconstruction");
});

test("a failed persona-id read or write never answers a wake without a stored binding", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const { ctx, counts, fail } = countingCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  const obj = new TestObject(ctx as never, env as never, state);

  // The initial binding write fails: the wake is refused, nothing runs.
  fail.put = true;
  const r1 = await obj.fetch(wakeReq());
  assert.equal(r1.status, 500);
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), undefined);
  assert.equal(ctx.alarmAt(), null);

  // The same instance retries the write on the next wake before serving.
  const r2 = await obj.fetch(wakeReq());
  assert.equal(r2.status, 200);
  await settle(ctx);
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), PERSONA);
  assert.equal(counts.put, 1);

  // A reconstructed instance whose storage read fails is refused too, then
  // binds from the stored id on the next wake without writing it again.
  const fresh = new TestObject(ctx as never, env as never, state);
  fail.get = true;
  const r3 = await fresh.fetch(wakeReq());
  assert.equal(r3.status, 500);
  state.addInput(PERSONA, "in-after-read-failure", "hello");
  const r4 = await fresh.fetch(wakeReq());
  assert.equal(r4.status, 200);
  await settle(ctx);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-after-read-failure")!.status,
    "done",
  );
  assert.equal(counts.put, 1);

  // The durable binding carries an alarm across a further reconstruction.
  state.addInput(PERSONA, "in-after-reconstruction", "hello");
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-after-reconstruction")!.status,
    "done",
  );
});

/** FakeState whose writer acquire or next-work fails while scripted. */
class FailingAcquireState extends FakeState {
  failWith: (() => Error) | null = null;
  nextWorkFails = false;
  override acquireWriter(persona: string, holder: string, ttlMs: number) {
    if (this.failWith) return Promise.reject(this.failWith());
    return super.acquireWriter(persona, holder, ttlMs);
  }
  override nextWork(persona: string) {
    if (this.nextWorkFails)
      return Promise.reject(new StateError(503, "state service 503"));
    return super.nextWork(persona);
  }
}

const HOUR = 3_600_000;

test("an authoritatively absent persona retires its alarm and storage; a created one wakes afresh", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);

  // The persona row disappears (reset, deletion). An alarm on a
  // reconstructed instance learns it and stops for good.
  state.personas.delete(PERSONA);
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(ctx.alarmAt(), null, "no re-arm for a nonexistent persona");
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), undefined);
  // A leftover alarm on the retired object finds nothing to serve.
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(ctx.alarmAt(), null);

  // A wake for the absent persona answers 404, stores nothing and deletes
  // an alarm that was still armed.
  await ctx.storage.setAlarm(Date.now() + HOUR);
  const obj = new TestObject(ctx as never, env as never, state);
  const r = await obj.fetch(wakeReq());
  assert.equal(r.status, 404);
  assert.equal(
    ((await r.json()) as { reason?: string }).reason,
    "persona_not_found",
  );
  await settle(ctx);
  assert.equal(ctx.alarmAt(), null);
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), undefined);

  // The same instance serves a persona created later under that id.
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-created", "hello");
  const r2 = await obj.fetch(wakeReq());
  assert.equal(r2.status, 200);
  await settle(ctx);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-created")!.status,
    "done",
  );
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), PERSONA);
});

test("a running persona deleted mid-life retires on its next alarm", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  const obj = new TestObject(ctx as never, env as never, state);
  await obj.fetch(wakeReq());
  await settle(ctx);
  state.personas.delete(PERSONA);
  await fire(ctx, obj);
  assert.equal(ctx.alarmAt(), null);
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), undefined);
});

test("a persona not active here sleeps without retrying and keeps its stored id", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);

  state.setPersonaAuthority(PERSONA, "transferred");
  await ctx.storage.setAlarm(Date.now() + HOUR);
  const r = await new TestObject(ctx as never, env as never, state).fetch(
    wakeReq(),
  );
  assert.equal(r.status, 409);
  assert.equal(
    ((await r.json()) as { reason?: string }).reason,
    "persona_inactive",
  );
  await settle(ctx);
  assert.equal(ctx.alarmAt(), null, "no retry cadence for a moved persona");
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), PERSONA);
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(ctx.alarmAt(), null);

  // Active again with work: the (sweep's) wake serves it as before.
  state.setPersonaAuthority(PERSONA, "active");
  state.addInput(PERSONA, "in-back", "hello again");
  const r2 = await new TestObject(ctx as never, env as never, state).fetch(
    wakeReq(),
  );
  assert.equal(r2.status, 200);
  await settle(ctx);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-back")!.status,
    "done",
  );
});

test("transient and ambiguous failures keep a retry alarm and the stored persona", async () => {
  const state = new FailingAcquireState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);

  const failures: Array<[string, () => Error]> = [
    ["outage", () => new StateError(503, "state service 503")],
    ["timeout", () => new StateError(503, "state call timed out")],
    ["uncoded 404", () => new StateError(404, "persona not found")],
    ["writer held", () => new StateError(409, "writer lease held")],
    ["uncoded 409", () => new StateError(409, "persona is not active")],
    ["internal", () => new StateError(500, "internal error")],
  ];
  for (const [label, err] of failures) {
    state.failWith = err;
    const t0 = Date.now();
    // Alarm path on a reconstructed instance.
    await fire(ctx, new TestObject(ctx as never, env as never, state));
    const armedIn = ctx.alarmAt()! - t0;
    assert.ok(armedIn >= 60_000 && armedIn < 61_000, `${label}: ${armedIn}`);
    assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), PERSONA, label);
    // Fetch path: 503, never 404/409, and a retry alarm stays armed.
    await ctx.storage.deleteAlarm();
    const r = await new TestObject(ctx as never, env as never, state).fetch(
      wakeReq(),
    );
    assert.equal(r.status, 503, label);
    await settle(ctx);
    assert.ok(ctx.alarmAt() !== null, label);
    assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), PERSONA, label);
  }

  // The plan itself cannot be read: retry rather than sleep.
  state.failWith = null;
  state.nextWorkFails = true;
  state.addInput(PERSONA, "in-plan-fails", "hello");
  const t0 = Date.now();
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-plan-fails")!.status,
    "done",
  );
  const retryIn = ctx.alarmAt()! - t0;
  assert.ok(retryIn >= 60_000 && retryIn < 61_000, `plan failure: ${retryIn}`);

  // The backend recovers: ordinary retry picks up waiting work, then sleeps.
  state.nextWorkFails = false;
  state.addInput(PERSONA, "in-recovered", "hello");
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-recovered")!.status,
    "done",
  );
  assert.equal(ctx.alarmAt(), null);
});

test("a failed retirement falls back to a retry alarm", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);
  state.personas.delete(PERSONA);

  const { deleteAll } = ctx.storage;
  ctx.storage.deleteAll = async () => {
    throw new Error("storage delete failed");
  };
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.ok(ctx.alarmAt() !== null, "re-armed to retry the retirement");
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), PERSONA);

  ctx.storage.deleteAll = deleteAll;
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(ctx.alarmAt(), null);
  assert.equal(await ctx.storage.get(PERSONA_KEY_FOR_TEST), undefined);
});

test("an idle persona sleeps: no alarm after a wake, none from a stray alarm", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-1", "hello");
  const ctx = fakeCtx();
  let sets = 0;
  const { setAlarm } = ctx.storage;
  ctx.storage.setAlarm = async (when: number) => {
    sets++;
    return setAlarm(when);
  };
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  const obj = new TestObject(ctx as never, env as never, state);
  await obj.fetch(wakeReq());
  await settle(ctx);
  assert.equal(ctx.alarmAt(), null);
  assert.equal(sets, 1, "only the in-flight guard was written");
  sets = 0;
  await fire(ctx, obj);
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(ctx.alarmAt(), null);
  assert.equal(sets, 0, "an idle alarm writes no alarm");
});

test("a fetch arms a guard while its drain runs; the plan replaces it", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.addInput(PERSONA, "in-slow", "!slow 200 hi");
  const ctx = fakeCtx();
  const env = { ...fakeEnv, SUMI_HEARTBEAT_MS: "60000" };
  const t0 = Date.now();
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  const guard = ctx.alarmAt()! - t0;
  assert.ok(guard >= 60_000 && guard < 61_000, `guard: ${guard}`);
  await settle(ctx);
  assert.equal(ctx.alarmAt(), null, "clean drain: guard replaced by no wake");
});

test("the plan wakes for backed-off inputs, schedules and turn-budget leftovers", async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  const ctx = fakeCtx();
  const env = {
    ...fakeEnv,
    SUMI_HEARTBEAT_MS: "60000",
    SUMI_DRAIN_TURN_BUDGET_MS: "100",
  };
  const near = (want: number, label: string) => {
    const got = ctx.alarmAt();
    assert.ok(
      got !== null && Math.abs(got - want) < 1_500,
      `${label}: armed ${got === null ? "nothing" : got - Date.now()}ms, want ${want - Date.now()}ms`,
    );
  };

  // A queued input backing off: the alarm waits for its not_before.
  state.addInput(PERSONA, "in-backoff", "later");
  const input = state.inputs.find((i) => i.input_id === "in-backoff")!;
  input.not_before = new Date(Date.now() + 10 * 60_000).toISOString();
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);
  near(Date.now() + 10 * 60_000, "not_before");
  input.status = "done";

  // A far schedule: the alarm waits for it — no heartbeat meanwhile.
  state.schedules.set(`${PERSONA}|far`, {
    persona_id: PERSONA,
    schedule_id: "far",
    wake_at: new Date(Date.now() + 3 * HOUR).toISOString(),
    payload: { text: "far" },
    miss_policy: "fire_late",
    status: "pending",
    claimed_generation: null,
    created_at: new Date().toISOString(),
    fired_at: null,
  });
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  near(Date.now() + 3 * HOUR, "schedule");

  // A fetch drain out of turn budget with work queued: an alarm drain
  // continues within the second, not at a heartbeat.
  state.addInput(PERSONA, "in-a", "!slow 200 a");
  state.addInput(PERSONA, "in-b", "b");
  await new TestObject(ctx as never, env as never, state).fetch(wakeReq());
  await settle(ctx);
  assert.equal(
    state.inputs.find((i) => i.input_id === "in-b")!.status,
    "queued",
  );
  near(Date.now() + 1_000, "leftover");
  await fire(ctx, new TestObject(ctx as never, env as never, state));
  assert.equal(state.inputs.find((i) => i.input_id === "in-b")!.status, "done");
  near(Date.now() + 3 * HOUR, "back to the schedule");
});

test("PersonaNotFoundError is what the fake raises for an unknown persona", async () => {
  const state = new FakeState();
  await assert.rejects(
    state.acquireWriter(PERSONA, "h", 1_000),
    PersonaNotFoundError,
  );
});

test("a branch paused without a retry deadline does not keep an idle alarm alive", {
  timeout: 3000,
}, async () => {
  const state = new FakeState();
  state.addPersona(PERSONA);
  state.listTools = async () => ["journal.note"];
  const provider = new BranchProvider();
  const ctx = fakeCtx();
  await twoExchanges(
    state,
    new TestObject(ctx as never, fakeEnv as never, state, provider),
    ctx,
  );
  await fire(
    ctx,
    new TestObject(ctx as never, fakeEnv as never, state, provider),
  );
  assert.equal(provider.branchRequests, 0);
  assert.equal(
    state.memoryBranches.get(`${PERSONA}|1`)!.state!.pause_reason,
    "private_tools_missing",
  );
  assert.equal(ctx.alarmAt(), null);
  assert.equal(
    state.inputs.filter((i) => i.kind === "memory_status").length,
    1,
  );
});
