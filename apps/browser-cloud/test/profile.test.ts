// ProfileBrowser lifecycle regressions for the 2026-09-27 independent review
// (F1, F2, F4, F6, F7, F8, F10). Adapted from the reviewer's harness
// (cloud-browser-review/artifacts/profile-harness.test.ts), which recorded the
// broken outcomes; these assert the corrected ones. Browser Run, CDP, the
// Sumi API and DO storage are fakes; no network.
import assert from "node:assert/strict";
import test from "node:test";
import { HUMAN_HOLD_MS, ProfileBrowser } from "../src/profile.ts";
import { RemoteBrowser } from "../src/remote-browser.ts";

const PROFILE = "11111111-1111-4111-8111-111111111111";
const PERSONA = "22222222-2222-4222-8222-222222222222";
const HUMAN = "55555555-5555-4555-8555-555555555555";

// biome-ignore lint/suspicious/noExplicitAny: the tests reach private members.
type Any = any;

function fakeRemote(opts: { collect?: () => unknown; onCollect?: (previous: unknown) => void } = {}) {
  const r: Any = {
    closed: false,
    active: "slot-a",
    lastFrame: undefined,
    inputs: [] as string[],
    async init() {},
    async startScreencast() {},
    async stopScreencast() {},
    tabs: () => [{ id: "slot-a", url: "https://a.example/", title: "A", active: true, loading: false }],
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
    async collect(previous: unknown) {
      opts.onCollect?.(previous);
      return (
        opts.collect?.() ?? {
          v: 1,
          at: Date.now(),
          cookies: [{ name: "sid", value: "x", domain: "a.example" }],
          origins: {},
          tabs: [{ slot: "slot-a", url: "https://a.example/", title: "A", scroll: { x: 0, y: 0 }, active: true }],
          notSaved: { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] },
        }
      );
    },
  };
  return r;
}

interface Options {
  storedSeq: number;
  incarnation: number;
  meta?: unknown;
  acquireFails?: boolean;
  snapshotStatus?: (seq: number) => number;
  grace?: string;
  alive?: string[];
  jevKeyVersion?: number;
}

/** One API, Browser Run and DO storage shared by successive ProfileBrowser
 * instances (a DO restart keeps storage; the API and browser live on). */
function world(o: Options) {
  const log: string[] = [];
  const store = new Map<string, unknown>();
  if (o.meta) store.set("meta", o.meta);
  let alarms = 0;
  const ctx = {
    storage: {
      async get(k: string) {
        return store.get(k);
      },
      async put(k: string, v: unknown) {
        store.set(k, structuredClone(v));
      },
      async delete(k: string) {
        return store.delete(k);
      },
      async setAlarm(_w: number) {
        alarms++;
      },
    },
  };
  let apiIncarnation = o.incarnation;
  let savedAt = new Date(Date.now() - 120_000).toISOString();
  let snapshot: unknown = {
    v: 1,
    at: 0,
    cookies: [],
    origins: { "https://carried.example": { localStorage: { k: "v" }, indexedDB: [] } },
    tabs: [],
    notSaved: { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] },
  };
  const api = {
    async fetch(url: string, init: RequestInit) {
      const op = url.split("/").pop();
      const body = JSON.parse(String(init.body ?? "{}"));
      log.push(`api:${op}:${JSON.stringify(body.seq ?? body.state ?? body.fresh ?? body.key_version ?? "")}`);
      if (op === "begin") {
        if (body.fresh) apiIncarnation++;
        else if (body.incarnation !== apiIncarnation) return new Response("{}", { status: 409 });
        return Response.json({
          profile_id: PROFILE, persona_id: PERSONA, enabled: true, incarnation: apiIncarnation, attachments: [],
          snapshot_seq: o.storedSeq, snapshot_version: 1, snapshot_at: savedAt, snapshot,
        });
      }
      if (op === "snapshot") {
        const status = o.snapshotStatus ? o.snapshotStatus(body.seq) : body.seq <= o.storedSeq || body.incarnation !== apiIncarnation ? 409 : 200;
        if (status !== 200) return new Response("{}", { status });
        o.storedSeq = body.seq;
        snapshot = body.snapshot;
        savedAt = new Date().toISOString();
        return Response.json({ seq: body.seq, saved_at: savedAt });
      }
      return Response.json({});
    },
  };
  const closed: string[] = [];
  const acquired: string[] = [];
  const alive = new Set(o.alive ?? []);
  const binding = {
    async acquire() {
      if (o.acquireFails) throw new Error("Browser Run: daily limit reached (429)");
      const id = `S-new-${acquired.length + 1}`;
      acquired.push(id);
      alive.add(id);
      return { sessionId: id };
    },
    async getSession(id: string) {
      return alive.has(id) ? { id } : null;
    },
    async closeSession(id: string) {
      closed.push(id);
      alive.delete(id);
    },
    async connectSession() {
      throw new Error("unused");
    },
  };
  const env = {
    BROWSER: binding, SUMI_STATE_URL: "http://api.invalid:8080", SUMI_STATE: api,
    SUMI_BROWSER_CLOUD_TOKEN: "t".repeat(40), SUMI_BROWSER_IDLE_GRACE_MS: o.grace,
  };
  return {
    log, closed, acquired, store, options: o,
    alarms: () => alarms,
    apiIncarnation: () => apiIncarnation,
    browser: (): Any => new ProfileBrowser(ctx as Any, env as Any),
  };
}

const wake = async (pb: Any, work: boolean) =>
  (await pb.fetch(new Request("https://profile.invalid/wake", { method: "POST", body: JSON.stringify({ profile: PROFILE, work }) }))).json();

const connectTo = (make: () => Any) => {
  (RemoteBrowser as Any).connect = async () => make();
};

// ---------- F2: reconnect continues the stored checkpoint ----------

test("F2: after a DO restart the reconnected browser continues checkpoint seq and carried origins, and stays live", async () => {
  const previous: Any[] = [];
  const remote = fakeRemote({ onCollect: (p) => previous.push(p) });
  connectTo(() => remote);
  const w = world({ storedSeq: 7, incarnation: 3, alive: ["S-live"], meta: { profile: PROFILE, session: "S-live", incarnation: 3, slots: { T1: "slot-a" }, ready: true } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  assert.equal(pb.recovery.kind, "reconnected");
  assert.equal(pb.checkpointSeq, 7);
  await pb.checkpoint("scheduled");
  assert.ok(w.log.includes("api:snapshot:8"), w.log.join(" "));
  assert.ok("https://carried.example" in previous[0].origins, "carried origins reach collect");
  assert.deepEqual(w.closed, [], "the live browser is not closed");
  assert.equal(pb.phase, "live");
  assert.equal(pb.remote, remote, "same live browser (same tab, page memory kept)");
  assert.equal(pb.meta.session, "S-live");

  // A second restart continues from 8.
  const again = w.browser();
  await wake(again, true);
  assert.equal(await again.ensureLive(), true);
  await again.checkpoint("scheduled");
  assert.ok(w.log.includes("api:snapshot:9"), w.log.join(" "));
  assert.deepEqual(w.closed, []);
  assert.equal(again.phase, "live");
  assert.equal(w.acquired.length, 0, "no new browser was acquired");
});

test("F2 control: a fresh start continues from the stored seq", async () => {
  connectTo(() => fakeRemote());
  const w = world({ storedSeq: 7, incarnation: 3, meta: { profile: PROFILE, incarnation: 3, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  await pb.checkpoint("scheduled");
  assert.ok(w.log.includes("api:snapshot:8"));
  assert.equal(pb.phase, "live");
});

// ---------- F1: a refused start changes nothing and does not loop ----------

test("F1: Browser Run refusal leaves the API untouched, arms no alarms for refresh wakes and backs off work wakes", async () => {
  connectTo(() => fakeRemote());
  const w = world({ storedSeq: 0, incarnation: 0, acquireFails: true, meta: { profile: PROFILE, incarnation: 0, slots: {} } });
  const pb = w.browser();
  assert.equal((await wake(pb, true)).accepted, true);
  await pb.alarm(); // runLoop: ensureLive fails -> break
  assert.equal(pb.phase, "unavailable");
  assert.equal(pb.phaseDetail, "browser_limit");
  assert.equal(w.apiIncarnation(), 0, "no begin(fresh): incarnation, tokens and state untouched");
  assert.equal(w.log.filter((l) => l.startsWith("api:begin")).length, 0);
  const before = w.alarms();
  for (let i = 0; i < 5; i++) {
    const answer = await wake(pb, false);
    assert.equal(answer.accepted, false);
    assert.equal(answer.phase, "unavailable", "the API drops the refresh on this answer");
    await pb.alarm();
  }
  assert.equal(w.alarms() - before, 0, "refresh wakes arm no alarm (no billed write)");
  for (let i = 0; i < 3; i++) {
    const answer = await wake(pb, true);
    assert.equal(answer.accepted, false);
    assert.ok(answer.retry_after_ms > 20_000, "work is held off");
    await pb.alarm();
  }
  assert.equal(w.alarms() - before, 0);
  assert.equal(w.apiIncarnation(), 0);
  assert.equal(pb.startFailures, 1);
  // The person's 再開 retries at once.
  const viewer = { send() {}, close() {} };
  pb.viewers.set(viewer, { human: HUMAN, authUntil: Date.now() + 60_000 });
  await pb.onViewerMessage(viewer, JSON.stringify({ type: "start" }));
  assert.equal(pb.startRetryAt, 0);
  assert.equal(w.alarms() - before, 1);
});

test("F1: a start failing after begin(fresh) reports the incarnation not live and closes the acquired browser", async () => {
  (RemoteBrowser as Any).connect = async () => {
    throw new Error("cdp connect failed");
  };
  const w = world({ storedSeq: 0, incarnation: 0, meta: { profile: PROFILE, incarnation: 0, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), false);
  assert.ok(w.log.includes('api:state:"sleeping"'), w.log.join(" "));
  assert.deepEqual(w.closed, ["S-new-1"]);
  assert.equal(pb.meta.session, undefined);
  assert.equal((w.store.get("meta") as Any).session, undefined);
  assert.equal(pb.phaseDetail, "start_failed");
});

// ---------- F4: a failed restore is closed, never reconnected to ----------

test("F4: a restore failing midway closes its browser; the next start restores again and only then checkpoints", async () => {
  let restores = 0;
  connectTo(() => {
    const r = fakeRemote();
    r.restore = async () => {
      restores++;
      if (restores === 1) throw new Error("page_script_failed: IndexedDB open blocked");
      return { origins: 1, tabs: 1, failedOrigins: [], failedRecords: 0 };
    };
    return r;
  });
  const w = world({ storedSeq: 7, incarnation: 3, meta: { profile: PROFILE, incarnation: 3, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), false);
  assert.deepEqual(w.closed, ["S-new-1"], "the half-restored browser is closed");
  assert.equal(pb.meta.session, undefined);
  assert.ok(w.log.includes('api:state:"sleeping"'));
  assert.equal(w.log.filter((l) => l.startsWith("api:snapshot")).length, 0, "checkpoint 7 is untouched");
  // The person presses 再開.
  assert.equal(await pb.ensureLive(), true);
  assert.equal(pb.recovery.kind, "restored");
  assert.equal(restores, 2, "restore ran again on a new browser");
  assert.equal(pb.meta.ready, true);
  await pb.checkpoint("scheduled");
  assert.ok(w.log.includes("api:snapshot:8"));
});

test("F4: after a DO restart a session whose start never finished is closed, not reconnected", async () => {
  connectTo(() => fakeRemote());
  const w = world({ storedSeq: 7, incarnation: 3, alive: ["S-half"], meta: { profile: PROFILE, session: "S-half", incarnation: 3, slots: {}, ready: false } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  assert.deepEqual(w.closed, ["S-half"]);
  assert.equal(pb.recovery.kind, "restored");
  assert.equal(pb.meta.session, "S-new-1");
});

// ---------- F8: failed saves back off ----------

test("F8: a failing checkpoint save backs off instead of re-collecting every loop pass", async () => {
  let collects = 0;
  connectTo(() => fakeRemote({ onCollect: () => collects++ }));
  const w = world({ storedSeq: 7, incarnation: 3, grace: "4000", snapshotStatus: () => 503, meta: { profile: PROFILE, incarnation: 3, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  pb.dirty = true;
  collects = 0;
  await pb.alarm(); // runs until the idle grace (4 s), then saves once more and sleeps
  const posts = w.log.filter((l) => l.startsWith("api:snapshot")).length;
  assert.ok(collects <= 3, `collects=${collects} (the frozen code made >= 6 in this window)`);
  assert.equal(posts, collects);
  assert.ok(pb.checkpointFailures >= 1);
});

// ---------- F6: key changes reach agents that were mid-tick ----------

test("F6: a Jev key changed during a tick is applied when the tick ends; the old key's rejection is not reported", async () => {
  connectTo(() => fakeRemote());
  const w = world({ storedSeq: 7, incarnation: 3, meta: { profile: PROFILE, incarnation: 3, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  const tab = { runtimeId: PROFILE, profileId: "cloud", tabId: "33333333-3333-4333-8333-333333333333" };
  const id = "44444444-4444-4444-8444-444444444444";
  const cred = { attachment: { attachment_id: id, persona_id: PERSONA, name: "t", tab, allow_actions: true, available: true }, host_token: "browser_x" };
  const session = (key?: string, version?: number, attachments: unknown[] = []) => ({
    profile_id: PROFILE, persona_id: PERSONA, enabled: true, incarnation: 4, attachments, snapshot_seq: 7, snapshot_version: 1,
    jev_key: key, jev_key_version: version,
  });
  pb.adopt(session("OLD-KEY-aaaaaaaa", 100, [cred]), false);
  const first = pb.agents.get(id);
  assert.equal(first.jevKey, "OLD-KEY-aaaaaaaa");
  first.ticking = new Promise(() => {}); // a poll or a goal is in flight
  pb.adopt(session("NEW-KEY-bbbbbbbb", 200), false);
  assert.equal(pb.agents.get(id), first, "not replaced mid-tick");
  // The old key was refused during that tick.
  Object.defineProperty(first.agent, "jevAvailable", { get: () => false });
  pb.afterTick(id, first);
  const after = pb.agents.get(id);
  assert.notEqual(after, first, "replaced when the tick ended");
  assert.equal(after.jevKey, "NEW-KEY-bbbbbbbb");
  assert.equal(after.agent.jevAvailable, true);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(w.log.filter((l) => l.startsWith("api:jev-rejected")).length, 0, "the new key is not reported rejected");
  // The current key refused: reported once, naming its version.
  Object.defineProperty(after.agent, "jevAvailable", { get: () => false });
  pb.afterTick(id, after);
  pb.afterTick(id, after);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.deepEqual(w.log.filter((l) => l.startsWith("api:jev-rejected")), ["api:jev-rejected:200"]);
  // Key deleted while idle: replaced at once.
  pb.adopt(session(undefined, undefined), false);
  assert.equal(pb.agents.get(id).agent.jevAvailable, false);
  assert.equal(pb.agents.get(id).jevKey, undefined);
});

// ---------- viewers: F7 (input tab) and F10 (control hold) ----------

class FakeSocket {
  static made: FakeSocket[] = [];
  sent: Any[] = [];
  closedWith?: [number | undefined, string | undefined];
  private readonly listeners: Record<string, ((event?: Any) => void)[]> = {};
  constructor() {
    FakeSocket.made.push(this);
  }
  accept() {}
  send(data: string) {
    this.sent.push(JSON.parse(data));
  }
  close(code?: number, reason?: string) {
    this.closedWith = [code, reason];
    this.emit("close");
  }
  addEventListener(type: string, listener: (event?: Any) => void) {
    this.listeners[type] ??= [];
    this.listeners[type].push(listener);
  }
  emit(type: string, event?: Any) {
    for (const listener of this.listeners[type] ?? []) listener(event);
  }
}

async function connectViewer(pb: Any, nonce: string): Promise<FakeSocket> {
  const g = globalThis as Any;
  g.WebSocketPair = class {
    0 = new FakeSocket();
    1 = new FakeSocket();
  };
  const NativeResponse = globalThis.Response;
  // Node's Response refuses status 101 (the Workers runtime needs it).
  g.Response = class extends NativeResponse {
    constructor(body?: BodyInit | null, init?: ResponseInit) {
      super(body, init?.status === 101 ? { ...init, status: 200 } : init);
    }
  };
  try {
    await pb.acceptViewer({ v: 1, h: HUMAN, p: PERSONA, b: PROFILE, s: 1, e: Date.now() + 60_000, n: nonce });
  } finally {
    g.Response = NativeResponse;
  }
  return FakeSocket.made[FakeSocket.made.length - 1] as FakeSocket;
}

const say = async (socket: FakeSocket, message: unknown) => {
  socket.emit("message", { data: JSON.stringify(message) });
  await new Promise((resolve) => setTimeout(resolve, 20));
};

test("F7: the person's input applies only to the tab whose frame they saw", async () => {
  const remote = fakeRemote();
  connectTo(() => remote);
  const w = world({ storedSeq: 7, incarnation: 3, meta: { profile: PROFILE, incarnation: 3, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  const socket = await connectViewer(pb, "n-f7");
  const click = { type: "input", kind: "mouse", event: "down", x: 10, y: 10, button: "left", buttons: 1, clickCount: 1 };
  // The secretary switched the screen to slot-b; the person clicked slot-a's frame.
  remote.active = "slot-b";
  await say(socket, { ...click, tab: "slot-a" });
  assert.deepEqual(remote.inputs, []);
  assert.ok(socket.sent.some((m) => m.type === "notice" && m.code === "tab_changed" && m.tab === "slot-b"));
  assert.equal(pb.control.mode, "agent", "refused input takes no control");
  await say(socket, click); // no tab at all
  assert.deepEqual(remote.inputs, []);
  await say(socket, { ...click, tab: "slot-b" });
  assert.deepEqual(remote.inputs, ["Input.dispatchMouseEvent"]);
  assert.equal(pb.control.mode, "human");
});

test("F10: a reload or brief disconnect keeps the person's control; staying away returns it to the secretary after the hold", async () => {
  connectTo(() => fakeRemote());
  const w = world({ storedSeq: 7, incarnation: 3, grace: "300", meta: { profile: PROFILE, incarnation: 3, slots: {} } });
  const pb = w.browser();
  await wake(pb, true);
  assert.equal(await pb.ensureLive(), true);
  let goalStops = 0;
  pb.agents.set("a1", {
    agent: { stopGoal: () => ++goalStops > 0, stop() {}, tick: async () => {}, jevAvailable: false },
    credential: { attachment: { tab: { tabId: "slot-a" }, allow_actions: true } },
    tickStarted: 0,
  });
  const first = await connectViewer(pb, "n-1");
  await say(first, { type: "takeover" });
  assert.equal(pb.control.mode, "human");
  assert.equal(goalStops, 1, "the running goal stopped on takeover");
  const epoch = pb.control.epoch;
  first.close(); // reload
  assert.equal(pb.control.mode, "human", "control is kept while the page reloads");
  assert.ok(pb.humanHoldUntil - Date.now() > HUMAN_HOLD_MS - 5_000);
  const second = await connectViewer(pb, "n-2");
  const hello = second.sent.find((m) => m.type === "hello");
  assert.equal(hello.control.mode, "human", "the reloaded page shows あなたが操作中");
  assert.equal(hello.control.holdMs, HUMAN_HOLD_MS);
  assert.equal(pb.humanHoldUntil, undefined);
  assert.equal(pb.control.epoch, epoch, "the secretary never had control in between");

  // Gone for longer than the hold: the browser stays up during the hold,
  // then control returns to the secretary; the stopped goal is not restarted.
  second.close();
  const loop = pb.alarm();
  await new Promise((resolve) => setTimeout(resolve, 800));
  assert.equal(pb.phase, "live", "the held browser does not idle-sleep");
  assert.equal(pb.control.mode, "human");
  pb.humanHoldUntil = Date.now() - 1;
  await loop;
  assert.equal(pb.controlReturned.reason, "viewer_absent");
  assert.equal(goalStops, 1);
  assert.equal(pb.phase, "sleeping", "then the idle grace applies as usual");
  // Explicit 秘書に戻す stays the ordinary path; its reason is its own.
  const pb2 = w.browser();
  await wake(pb2, true);
  assert.equal(await pb2.ensureLive(), true);
  const third = await connectViewer(pb2, "n-3");
  await say(third, { type: "takeover" });
  await say(third, { type: "release" });
  assert.equal(pb2.control.mode, "agent");
  assert.equal(pb2.controlReturned.reason, "person_release");
  assert.ok(third.sent.some((m) => m.type === "control" && m.mode === "agent" && m.reason === "person_release"));
  third.close();
  assert.equal(pb2.humanHoldUntil, undefined, "no hold once control is the secretary's");
});
