// RemoteBrowser checkpoint budget (review F5) and per-origin restore
// isolation (review F3), exercised on the real collect/restore methods with a
// fake CDP side. The page scripts themselves run in real Chrome in
// storage-scripts.chrome.test.ts.
import assert from "node:assert/strict";
import test from "node:test";
import { MAX_SNAPSHOT_BYTES, RemoteBrowser, type Snapshot } from "../src/remote-browser.ts";

// biome-ignore lint/suspicious/noExplicitAny: fake `this` for the real methods.
type Any = any;

const slot = (id: string, url: string) => ({ id, targetId: `t-${id}`, sessionId: `s-${id}`, frameId: `f-${id}`, url, title: id, revision: 1, loading: false, busy: false, crashed: false });

/** A page whose storage is `bytes` of localStorage; COLLECT gives up when
 * its MAX is smaller, as the real script does. */
function collectingBrowser(pages: Record<string, number>, active: string) {
  const slots = new Map(Object.keys(pages).map((url, i) => [`slot-${i}`, slot(`slot-${i}`, url)]));
  const self: Any = {
    slots,
    active,
    cdp: { send: async (method: string) => (method === "Storage.getCookies" ? { cookies: [{ name: "sid", value: "signed-in", domain: "small.example" }] } : {}) },
    async evaluate(id: string, expression: string) {
      if (expression.includes("scrollX")) return { x: 0, y: 0 };
      const max = Number(/const MAX = (\d+);/.exec(expression)?.[1]);
      const bytes = pages[(slots.get(id) as Any).url] as number;
      if (bytes > max) return { tooLarge: true, bytes: max + 1 };
      return { localStorage: { cache: "x".repeat(bytes) }, indexedDB: [], nonJsonValues: 1, sessionStorageKeys: 2, bytes };
    },
  };
  return self;
}

test("F5: one origin over the budget is reported; cookies, tabs and the other origins are saved", async () => {
  const self = collectingBrowser({ "https://heavy.example/": 2_200_000, "https://small.example/app": 1_000 }, "slot-0");
  const previous: Snapshot = {
    v: 1, at: 0, cookies: [], tabs: [],
    origins: { "https://carried.example": { localStorage: { k: "v" }, indexedDB: [] } },
    notSaved: { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] },
  };
  const snapshot: Snapshot = await RemoteBrowser.prototype.collect.call(self, previous, MAX_SNAPSHOT_BYTES);
  assert.deepEqual(snapshot.notSaved.oversizedOrigins, ["https://heavy.example"]);
  assert.deepEqual(Object.keys(snapshot.origins).sort(), ["https://carried.example", "https://small.example"]);
  assert.equal(snapshot.cookies.length, 1, "sign-in cookies are kept");
  assert.equal(snapshot.tabs.length, 2, "both tabs are kept");
  assert.equal(snapshot.notSaved.nonJsonIdbValues, 1);
  assert.ok(new TextEncoder().encode(JSON.stringify(snapshot)).length <= MAX_SNAPSHOT_BYTES);
});

test("F5: origins share the budget; the active tab's origin comes first and carried data never overflows it", async () => {
  const self = collectingBrowser({ "https://a.example/": 900_000, "https://b.example/": 900_000 }, "slot-1");
  const previous: Snapshot = {
    v: 1, at: 0, cookies: [], tabs: [],
    origins: { "https://old.example": { localStorage: { big: "y".repeat(900_000) }, indexedDB: [] } },
    notSaved: { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] },
  };
  const snapshot: Snapshot = await RemoteBrowser.prototype.collect.call(self, previous, MAX_SNAPSHOT_BYTES);
  assert.deepEqual(Object.keys(snapshot.origins).sort(), ["https://a.example", "https://b.example"]);
  assert.deepEqual(snapshot.notSaved.oversizedOrigins, ["https://old.example"]);
  const small = await RemoteBrowser.prototype.collect.call(collectingBrowser({ "https://a.example/": 900_000, "https://b.example/": 900_000 }, "slot-1"), undefined, 1_000_000);
  assert.deepEqual(Object.keys(small.origins), ["https://b.example"], "the active tab's origin wins");
  assert.deepEqual(small.notSaved.oversizedOrigins, ["https://a.example"]);
});

test("F3: an origin whose restore fails is skipped; the other origins and the tabs are restored", async () => {
  const first = slot("blank", "about:blank");
  let current = "";
  const restored: string[] = [];
  const self: Any = {
    slots: new Map([["blank", first]]),
    byTarget: new Map(),
    bySession: new Map(),
    pendingSlots: [],
    restoring: false,
    active: undefined,
    cdp: { send: async () => ({}) },
    async navigate(_id: string, url: string) {
      current = url;
    },
    async waitLoaded() {},
    async evaluate(_id: string, expression: string) {
      if (current.startsWith("https://binary-key.example")) throw new Error("DataError: not a valid key");
      if (expression.includes("scrollTo")) return undefined;
      restored.push(new URL(current).origin);
      return { failedRecords: 2 };
    },
    async activate(id: string) {
      this.active = id;
    },
  };
  const snapshot: Snapshot = {
    v: 1, at: 0, cookies: [],
    origins: {
      "https://binary-key.example": { localStorage: {}, indexedDB: [] },
      "https://good.example": { localStorage: { k: "v" }, indexedDB: [] },
    },
    tabs: [{ slot: "slot-9", url: "https://good.example/", title: "Good", scroll: { x: 0, y: 0 }, active: true }],
    notSaved: { nonJsonIdbValues: 0, sessionStorageKeys: 0, skippedOrigins: [] },
  };
  const result = await RemoteBrowser.prototype.restore.call(self, snapshot);
  assert.deepEqual(result, { origins: 1, tabs: 1, failedOrigins: ["https://binary-key.example"], failedRecords: 2 });
  assert.deepEqual(restored, ["https://good.example"]);
  assert.equal(self.active, "slot-9", "the tab returns to its stable slot");
});
