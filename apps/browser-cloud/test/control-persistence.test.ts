// Review-02 N1: the person's takeover survives a DO restart (deploy, reset,
// eviction) that reattaches the same live browser. Adapted from the
// reviewer's harness (cloud-browser-review/artifacts/repair-review-02/
// repair-harness.test.ts), which showed control silently back with the
// secretary and its navigation dispatched; these assert the corrected
// outcomes. Browser Run, CDP, the Sumi API and DO storage are fakes.
import assert from "node:assert/strict";
import test from "node:test";
import { HUMAN_HOLD_MS, ProfileBrowser } from "../src/profile.ts";
import { RemoteBrowser } from "../src/remote-browser.ts";

// biome-ignore lint/suspicious/noExplicitAny: the tests reach private members.
type Any = any;
const PROFILE = "11111111-1111-4111-8111-111111111111";
const PERSONA = "22222222-2222-4222-8222-222222222222";
const HUMAN = "55555555-5555-4555-8555-555555555555";
const TAB = { runtimeId: PROFILE, profileId: "cloud", tabId: "slot-a" };
const PAY = "https://a.example/pay";
const LIVE_META = { profile: PROFILE, session: "S-live", incarnation: 3, slots: { T1: "slot-a" }, ready: true };

function fakeRemote() {
  const slot = { id: "slot-a", targetId: "T1", sessionId: "s1", frameId: "f1", url: "https://a.example/", title: "A", revision: 1, loading: false, busy: false, crashed: false };
  const r: Any = {
    closed: false, active: "slot-a", lastFrame: undefined, inputs: [] as string[], navigations: [] as string[],
    async init() {}, async startScreencast() {}, async stopScreencast() {},
    tabs: () => [{ id: "slot-a", url: slot.url, title: "A", active: true, loading: false }],
    async heartbeat() {},
    close() {
      r.closed = true;
    },
    targetsBySlot: () => ({ T1: "slot-a" }),
    async restore() {
      return { origins: 0, tabs: 1, failedOrigins: [], failedRecords: 0 };
    },
    async input(method: string) {
      r.inputs.push(method);
    },
    slot: (id: string) => (id === slot.id ? slot : undefined),
    async activate(id: string) {
      r.active = id;
    },
    async evaluate(_id: string, expr: string) {
      return expr === "location.href" ? slot.url : { title: "A", text: "Transfer form", targets: [] };
    },
    async navigate(_id: string, url: string) {
      r.navigations.push(url);
    },
    async collect() {
      return { v: 1, at: Date.now(), cookies: [], origins: {}, tabs: [], notSaved: { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] } };
    },
  };
  return r;
}

/** One DO storage, API and Browser Run shared by successive instances. */
function world(meta: unknown, grace?: string) {
  const store = new Map<string, unknown>([["meta", meta]]);
  const controlWrites: Any[] = [];
  const hooks: { failNext: number; gate?: Promise<void> } = { failNext: 0 };
  const ctx = {
    storage: {
      async get(k: string) {
        return structuredClone(store.get(k));
      },
      async put(k: string, v: unknown) {
        if (k === "control") {
          if (hooks.gate) await hooks.gate;
          if (hooks.failNext > 0) {
            hooks.failNext--;
            throw new Error("storage unavailable");
          }
          controlWrites.push(structuredClone(v));
        }
        store.set(k, structuredClone(v));
      },
      async delete(k: string) {
        return store.delete(k);
      },
      async setAlarm() {},
    },
  };
  let incarnation = 3;
  const api = {
    async fetch(url: string, init: RequestInit) {
      const op = url.split("/").pop();
      const body = JSON.parse(String(init.body ?? "{}"));
      if (op === "begin") {
        if (body.fresh) incarnation++;
        return Response.json({ profile_id: PROFILE, persona_id: PERSONA, enabled: true, incarnation, attachments: [], snapshot_seq: 7, snapshot_version: 1 });
      }
      if (op === "snapshot") return Response.json({ seq: body.seq, saved_at: new Date().toISOString() });
      return Response.json({});
    },
  };
  const alive = new Set(["S-live"]);
  const binding = {
    async acquire() {
      alive.add("S-fresh");
      return { sessionId: "S-fresh" };
    },
    async getSession(id: string) {
      return alive.has(id) ? { id } : null;
    },
    async closeSession(id: string) {
      alive.delete(id);
    },
    async connectSession() {
      throw new Error("unused");
    },
  };
  const env = { BROWSER: binding, SUMI_STATE_URL: "http://api.invalid:8080", SUMI_STATE: api, SUMI_BROWSER_CLOUD_TOKEN: "t".repeat(40), SUMI_BROWSER_IDLE_GRACE_MS: grace };
  return { store, controlWrites, hooks, alive, browser: (): Any => new ProfileBrowser(ctx as Any, env as Any) };
}

class FakeSocket {
  sent: Any[] = [];
  private readonly l: Record<string, ((e?: Any) => void)[]> = {};
  accept() {}
  send(d: string) {
    this.sent.push(JSON.parse(d));
  }
  close() {
    this.emit("close");
  }
  addEventListener(t: string, f: (e?: Any) => void) {
    this.l[t] ??= [];
    this.l[t].push(f);
  }
  emit(t: string, e?: Any) {
    for (const f of this.l[t] ?? []) f(e);
  }
}

async function connectViewer(pb: Any, n: string): Promise<FakeSocket> {
  const g = globalThis as Any;
  const made = new FakeSocket();
  g.WebSocketPair = class {
    0 = new FakeSocket();
    1 = made;
  };
  const Native = globalThis.Response;
  g.Response = class extends Native {
    constructor(b?: Any, i?: ResponseInit) {
      super(b, i?.status === 101 ? { ...i, status: 200 } : i);
    }
  };
  try {
    await pb.acceptViewer({ v: 1, h: HUMAN, p: PERSONA, b: PROFILE, s: 1, e: Date.now() + 60_000, n });
  } finally {
    g.Response = Native;
  }
  return made;
}

const tick = (ms = 20) => new Promise((r) => setTimeout(r, ms));
const say = async (s: FakeSocket, m: unknown) => {
  s.emit("message", { data: JSON.stringify(m) });
  await tick();
};
const lastHello = (s: FakeSocket) => s.sent.filter((m) => m.type === "hello").pop();
async function secretaryNavigates(pb: Any) {
  const o = await pb.port.observe(TAB);
  return pb.port.act(TAB, o.binding, { kind: "navigate", url: PAY });
}
const click = { type: "input", kind: "mouse", event: "down", x: 10, y: 10, button: "left", buttons: 1, clickCount: 1, tab: "slot-a" };

async function takenOver(w: ReturnType<typeof world>, remote: Any) {
  (RemoteBrowser as Any).connect = async () => remote;
  const pb = w.browser();
  const v = await connectViewer(pb, `n-${Math.random()}`);
  assert.equal(await pb.ensureLive(), true);
  await say(v, { type: "takeover" });
  assert.equal(pb.control.mode, "human");
  return { pb, v };
}

test("N1: the takeover survives a DO restart that reattaches the same live browser; the secretary is refused until 秘書に戻す", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META);
  const { pb: before, v: v1 } = await takenOver(w, remote);
  const epoch = before.control.epoch;
  // Repeated input writes nothing more (one write per transition).
  await say(v1, click);
  await say(v1, { ...click, event: "up" });
  assert.equal(remote.inputs.length, 2);
  assert.deepEqual(w.controlWrites.map((c) => c.mode), ["human"]);
  await assert.rejects(secretaryNavigates(before), /person took control/);

  // Deploy / DO reset: memory gone, storage and the browser live on; the old
  // sockets die without their close callback running in the new instance.
  const after = w.browser();
  const v2 = await connectViewer(after, "n-after");
  assert.equal(await after.ensureLive(), true);
  assert.equal(after.recovery.kind, "reconnected");
  assert.equal(after.remote, remote, "same live browser");
  assert.equal(after.control.mode, "human");
  assert.equal(after.control.epoch, epoch, "same fencing epoch (observations of the old instance are void anyway)");
  const hello = lastHello(v2);
  assert.equal(hello.phase, "live");
  assert.deepEqual(hello.control, { mode: "human", holdMs: HUMAN_HOLD_MS });
  assert.equal(after.humanHoldUntil, undefined, "the person is connected: no hold runs");
  await assert.rejects(secretaryNavigates(after), /person took control/);
  assert.deepEqual(remote.navigations, [], "nothing reached the page");

  // Only 秘書に戻す gives it back, with its reason.
  await say(v2, { type: "release" });
  assert.equal(after.control.mode, "agent");
  assert.ok(v2.sent.some((m) => m.type === "control" && m.mode === "agent" && m.reason === "person_release"));
  const receipt = await secretaryNavigates(after);
  assert.equal(receipt.status, "dispatched");
  assert.deepEqual(remote.navigations, [PAY]);
  await tick();
  // takeover, hold started at restart, hold cleared on reconnect, release.
  assert.deepEqual(
    w.controlWrites.map((c) => [c.mode, c.holdUntil === undefined ? "-" : "hold", c.returned?.reason ?? "-"]),
    [["human", "-", "-"], ["human", "hold", "-"], ["human", "-", "-"], ["agent", "-", "person_release"]],
  );
});

test("N1: restart during the hold keeps the original deadline; expiry returns control with its reason", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META, "300");
  const { v } = await takenOver(w, remote);
  v.close(); // reload / network loss
  await tick();
  const deadline = (w.store.get("control") as Any).holdUntil;
  assert.ok(deadline > Date.now() + HUMAN_HOLD_MS - 5_000);

  const after = w.browser();
  assert.equal(await after.ensureLive(), true);
  assert.equal(after.control.mode, "human");
  assert.equal(after.humanHoldUntil, deadline, "the hold is not extended by the restart");
  await assert.rejects(secretaryNavigates(after), /person took control/);

  // The deadline passes with nobody back.
  after.humanHoldUntil = Date.now() - 1;
  await after.alarm(); // releases, then idles to sleep (grace 300 ms)
  assert.equal(after.control.mode, "agent");
  assert.equal(after.controlReturned.reason, "viewer_absent");
  assert.deepEqual((w.store.get("control") as Any).returned?.reason, "viewer_absent");
  // A later instance still tells the person why.
  const later = w.browser();
  const v3 = await connectViewer(later, "n-later");
  assert.equal(lastHello(v3).control.mode, "agent");
  assert.equal(lastHello(v3).control.returned.reason, "viewer_absent");
});

test("N1: a hold that expired while the DO was down returns control at restore, before the secretary acts", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META);
  await takenOver(w, remote);
  w.store.set("control", { ...(w.store.get("control") as Any), holdUntil: Date.now() - 1_000 });
  const after = w.browser();
  assert.equal(await after.ensureLive(), true);
  assert.equal(after.control.mode, "agent");
  assert.equal(after.controlReturned.reason, "viewer_absent");
  const receipt = await secretaryNavigates(after);
  assert.equal(receipt.status, "dispatched");
});

test("N1: restart while the person was connected starts the hold at restore; nobody back means the secretary waits", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META);
  await takenOver(w, remote); // viewer never closed: the old instance just vanished
  const t0 = Date.now();
  const after = w.browser();
  assert.equal(await after.ensureLive(), true);
  assert.equal(after.control.mode, "human");
  assert.ok(after.humanHoldUntil >= t0 + HUMAN_HOLD_MS && after.humanHoldUntil <= Date.now() + HUMAN_HOLD_MS);
  assert.equal((w.store.get("control") as Any).holdUntil, after.humanHoldUntil, "the started hold is durable");
  await assert.rejects(secretaryNavigates(after), /person took control/);
});

test("N1: a fresh browser (the old one is gone) keeps the person's control; stopped goals stay stopped", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META);
  (RemoteBrowser as Any).connect = async () => remote;
  const pb = w.browser();
  const v1 = await connectViewer(pb, "n-goal");
  assert.equal(await pb.ensureLive(), true);
  let goalStops = 0;
  pb.agents.set("a1", { agent: { stopGoal: () => ++goalStops > 0, stop() {} }, credential: { attachment: { tab: TAB, allow_actions: true } }, tickStarted: 0 });
  await say(v1, { type: "takeover" }); // stops the running goal
  await say(v1, { type: "takeover" }); // already the person's: no-op
  assert.equal(goalStops, 1);
  w.alive.delete("S-live"); // browser lost with the DO
  const after = w.browser();
  const v = await connectViewer(after, "n-fresh");
  assert.equal(await after.ensureLive(), true);
  assert.equal(after.recovery.kind, "fresh", "a new browser (no checkpoint in this fake)");
  assert.equal(after.meta.session, "S-fresh");
  assert.equal(after.control.mode, "human");
  assert.equal(lastHello(v).control.mode, "human");
  await assert.rejects(secretaryNavigates(after), /person took control/);
  assert.equal(goalStops, 1, "the stopped goal was not revived (the new instance's agents come from the API)");
});

test("N1: the person's input reaches the page only after the takeover is durable; the secretary is fenced at once", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META);
  (RemoteBrowser as Any).connect = async () => remote;
  const pb = w.browser();
  const v = await connectViewer(pb, "n-order");
  assert.equal(await pb.ensureLive(), true);
  const o = await pb.port.observe(TAB);
  let open!: () => void;
  w.hooks.gate = new Promise<void>((r) => {
    open = r;
  });
  v.emit("message", { data: JSON.stringify(click) });
  await tick();
  assert.equal(pb.control.mode, "human", "fenced in memory before the write lands");
  await assert.rejects(pb.port.act(TAB, o.binding, { kind: "navigate", url: PAY }), /person took control/);
  assert.deepEqual(remote.inputs, [], "the click waits for the durable takeover");
  open();
  w.hooks.gate = undefined;
  await tick();
  assert.deepEqual(remote.inputs, ["Input.dispatchMouseEvent"]);
  assert.equal((w.store.get("control") as Any).mode, "human");
});

test("N1: a failed control write keeps the person in control, says so, and is retried by the loop", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META, "300");
  (RemoteBrowser as Any).connect = async () => remote;
  const pb = w.browser();
  const v = await connectViewer(pb, "n-fail");
  assert.equal(await pb.ensureLive(), true);
  w.hooks.failNext = 1;
  await say(v, { type: "takeover" });
  assert.equal(pb.control.mode, "human");
  assert.equal(pb.controlUnsaved, true);
  assert.ok(v.sent.some((m) => m.type === "notice" && m.code === "control_not_saved"));
  assert.equal(w.store.get("control"), undefined);
  await assert.rejects(secretaryNavigates(pb), /person took control/);
  // The loop retries (every 5 s while unsaved); run it until the write lands.
  const loop = pb.alarm();
  await tick(600);
  assert.equal(pb.controlUnsaved, false);
  assert.equal((w.store.get("control") as Any).mode, "human");
  v.close();
  pb.humanHoldUntil = Date.now() - 1;
  await loop;
  assert.equal(pb.control.mode, "agent");
});

test("N1: deleting the browser clears the takeover, even with a control write still queued", async () => {
  const remote = fakeRemote();
  const w = world(LIVE_META);
  const { pb, v } = await takenOver(w, remote);
  let open!: () => void;
  w.hooks.gate = new Promise<void>((r) => {
    open = r;
  });
  v.close(); // queues the hold-start write behind the gate
  const retiring = pb.retire(pb.meta);
  await tick();
  open();
  w.hooks.gate = undefined;
  await retiring;
  assert.equal(w.store.get("control"), undefined);
  const later = w.browser();
  await later.loadMeta();
  assert.equal(later.control.mode, "agent");
});
