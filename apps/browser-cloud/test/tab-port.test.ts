import assert from "node:assert/strict";
import test from "node:test";
import type { BrowserRuntimeError, TabRef } from "@sumi/desktop/browser-contract";
import type { RemoteBrowser, Slot } from "../src/remote-browser.ts";
import { type AgentActivity, CloudTabPort, type Control, pageScript } from "../src/tab-port.ts";

const PROFILE = "00000000-0000-4000-8000-000000000001";

function fixture() {
  const slot: Slot = {
    id: "slot-1", targetId: "t1", sessionId: "s1", frameId: "f1", url: "http://app.sumi-fixture.test/",
    title: "Fixture", revision: 3, loading: false, busy: false, crashed: false,
  };
  const log: string[] = [];
  let href = slot.url;
  const remote = {
    closed: false,
    active: "slot-1",
    slot: (id: string) => (id === slot.id ? slot : undefined),
    async activate(id: string) {
      log.push(`activate:${id}`);
      this.active = id;
    },
    async navigate(_id: string, url: string) {
      log.push(`navigate:${url}`);
      slot.revision++;
      href = url;
    },
    async evaluate(_id: string, expression: string) {
      if (expression === "location.href") return href;
      const request = JSON.parse(/\)\((\{.*\})\); \}\)\(\)$/s.exec(expression)?.[1] ?? "null");
      log.push(`page:${request.kind}${request.action ? `:${request.action.kind}` : ""}`);
      if (request.kind === "observe")
        return { title: "Fixture", text: "Hello", targets: [{ id: "t0", role: "button", name: "Count +1", bounds: { x: 1, y: 2, width: 3, height: 4 } }], truncated: false };
      if (hooks.beforeAct) hooks.beforeAct();
      return { ok: true };
    },
  };
  const hooks: { beforeAct?: () => void } = {};
  let control: Control = { mode: "agent", epoch: 0 };
  let humanAt = 0;
  const intents: unknown[] = [];
  const activity: AgentActivity[] = [];
  const port = new CloudTabPort({
    browser: () => remote as unknown as RemoteBrowser,
    profileId: () => PROFILE,
    control: () => control,
    humanInputAt: () => humanAt,
    recordIntent: async (intent) => {
      intents.push(intent);
      log.push(intent ? "intent" : "intent-cleared");
    },
    activity: (event) => activity.push(event),
  });
  const ref: TabRef = { runtimeId: PROFILE, profileId: "cloud", tabId: "slot-1" };
  return {
    slot, remote, port, ref, log, intents, activity, hooks,
    takeover() {
      control = { mode: "human", epoch: control.epoch + 1 };
      humanAt = Date.now();
    },
    touch() {
      humanAt = Date.now() + 1;
    },
  };
}

const code = async (promise: Promise<unknown>) => {
  try {
    await promise;
  } catch (error) {
    return (error as BrowserRuntimeError).code;
  }
  return "ok";
};

test("observe then act dispatches once, with a durable intent around the dispatch", async () => {
  const f = fixture();
  const obs = await f.port.observe(f.ref);
  assert.equal(obs.binding.revision, 3);
  const receipt = await f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" });
  assert.equal(receipt.status, "dispatched");
  assert.deepEqual(f.log, ["page:observe", "intent", "page:act:click", "intent-cleared"]);
  assert.deepEqual(f.activity.map((a) => `${a.phase}:${a.outcome ?? ""}`), ["start:", "end:dispatched"]);
  assert.equal(f.activity[0]?.label, "Count +1");
  // Single use.
  assert.equal(await code(f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" })), "stale_observation");
});

test("a takeover after the observation refuses the action before dispatch", async () => {
  const f = fixture();
  const obs = await f.port.observe(f.ref);
  f.takeover();
  assert.equal(await code(f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" })), "page_changed");
  assert.ok(!f.log.includes("page:act:click"));
  assert.ok(!f.log.includes("intent"));
});

test("guarded actions refuse after any person input; unguarded ones only after a takeover", async () => {
  const f = fixture();
  const obs = await f.port.observe(f.ref);
  f.touch();
  assert.equal(await code(f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" }, { guard: true })), "page_changed");
  const again = await f.port.observe(f.ref);
  f.touch();
  assert.equal(await code(f.port.act(f.ref, again.binding, { kind: "click", target: "t0" })), "ok");
});

test("navigation since the observation or a different profile is refused", async () => {
  const f = fixture();
  const obs = await f.port.observe(f.ref);
  f.slot.revision++;
  assert.equal(await code(f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" })), "stale_observation");
  assert.equal(await code(f.port.observe({ ...f.ref, runtimeId: "00000000-0000-4000-8000-000000000002" })), "wrong_runtime");
  f.slot.loading = true;
  assert.equal(await code(f.port.observe(f.ref)), "tab_navigating");
});

test("an invalidated port (reconnect, new incarnation) forgets every observation", async () => {
  const f = fixture();
  const obs = await f.port.observe(f.ref);
  f.port.invalidate();
  assert.equal(await code(f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" })), "stale_observation");
});

test("a failure after dispatch keeps the intent so the outcome stays unknown", async () => {
  const f = fixture();
  const obs = await f.port.observe(f.ref);
  f.hooks.beforeAct = () => {
    throw new Error("socket closed");
  };
  f.remote.closed = false;
  assert.equal(await code(f.port.act(f.ref, obs.binding, { kind: "click", target: "t0" })), "tab_unavailable");
  assert.equal(f.intents.at(-1) !== null, true, "intent not cleared");
  assert.equal(f.activity.at(-1)?.outcome, "failed");
});

test("the acting tab is brought to the person's screen first", async () => {
  const f = fixture();
  f.remote.active = "other";
  const obs = await f.port.observe(f.ref);
  await f.port.act(f.ref, obs.binding, { kind: "navigate", url: "http://docs.sumi-fixture.test/" });
  assert.deepEqual(f.log.slice(1), ["activate:slot-1", "intent", "navigate:http://docs.sumi-fixture.test/", "intent-cleared"]);
});

test("the page script carries its own __name shim (Worker bundles call it)", () => {
  const script = pageScript({ kind: "observe" });
  // biome-ignore lint/security/noGlobalEval: evaluates the generated expression shape only.
  const run = new Function("pageOperationStub", `return ${script.replace(/return \(.*\)\((\{.*\})\); \}\)\(\)$/s, "return __name(pageOperationStub, 'x')($1); })()")}`);
  assert.equal(run((request: { kind: string }) => request.kind), "observe");
});
