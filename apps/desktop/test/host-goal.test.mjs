import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { BrowserHostAgent } from "../dist/browser/host.js";
import { JevClient } from "../dist/browser/jev.js";
import { choice, FIXTURE_KEY, startJevFixture } from "./jev-fixture.mjs";

const tab = { runtimeId: "runtime", profileId: "profile", tabId: "tab" };
const credential = {
  attachment: { attachment_id: "attachment", tab, allow_actions: true },
  host_token: "host-secret-never-to-page",
};
const goalJob = () => ({
  job_id: "goal-job",
  claim_expires_at: new Date(Date.now() + 30000).toISOString(),
  request: {
    attachment_id: "attachment",
    tab,
    method: "goal",
    goal: "Press Next until the end",
    inputs: {},
    max_steps: 5,
  },
});
const page = () => {
  let n = 0;
  const acts = [];
  return {
    acts,
    observe: async () => ({
      tab,
      binding: { revision: 0, observationId: `o${++n}`, url: "http://x.test/" },
      title: "Pager\0with NUL",
      text: `page ${acts.length}`,
      truncated: false,
      targets: [
        { id: "t0", tag: "button", role: "", name: "Next", bounds: {} },
        { id: "t1", tag: "a", role: "", name: "Help", bounds: {} },
      ],
    }),
    act: async (_t, _b, action, options) => {
      assert.equal(options.guard, true);
      acts.push(action);
      return { tab, status: "dispatched", revision: 0 };
    },
  };
};
const nextPolicy = (body) => ({
  operation: choice(Object.keys(body.questions.operation.criteria), "CLICK"),
  click_target: choice(["t0", "t1"], "t0"),
});

async function readBody(req) {
  let s = "";
  for await (const b of req) s += b;
  return s ? JSON.parse(s) : {};
}

async function run(t, { jev, progress }) {
  const calls = { polls: [], progress: [], receipts: [] };
  let served = false;
  const server = createServer(async (req, res) => {
    const body = await readBody(req);
    if (req.url.endsWith("/poll")) {
      calls.polls.push(body);
      res.end(JSON.stringify({ job: served ? null : goalJob() }));
      served = true;
    } else if (req.url.endsWith("/progress")) {
      calls.progress.push(body);
      const reply = progress(calls.progress.length);
      res.statusCode = reply.statusCode ?? 200;
      res.end(JSON.stringify(reply.body ?? {}));
    } else {
      calls.receipts.push(body);
      res.end("{}");
    }
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(() => {
    server.closeAllConnections();
    server.close();
  });
  const browser = page();
  const agent = new BrowserHostAgent({
    apiOrigin: `http://127.0.0.1:${server.address().port}`,
    credential,
    browser,
    tab,
    jev,
  });
  await agent.tick();
  return { calls, browser };
}

test("host declares Jev and cancels a running goal before its next action", async (t) => {
  const fixture = await startJevFixture({ policy: nextPolicy });
  t.after(fixture.close);
  const jev = new JevClient({
    apiKey: FIXTURE_KEY,
    endpoint: fixture.endpoint,
  });
  const { calls, browser } = await run(t, {
    jev,
    progress: (n) => ({
      body: { status: n < 3 ? "running" : "cancel_requested" },
    }),
  });
  assert.deepEqual(calls.polls[0], { jev: true });
  assert.equal(browser.acts.length, 2);
  const [receipt] = calls.receipts;
  assert.equal(receipt.status, "cancelled");
  assert.equal(receipt.result.value.goal_outcome, "cancelled");
  assert.equal(receipt.result.dispatched, true);
  assert.equal(calls.progress[0].progress.phase, "acting");
  assert.ok(calls.progress[0].progress.next.target.includes("Next"));
  const everything = JSON.stringify(calls);
  assert.ok(!everything.includes(FIXTURE_KEY));
  // Website text with NUL is sanitized before it reaches JSONB.
  assert.ok(!everything.includes("\\u0000"));
  assert.equal(calls.progress[0].progress.title, "Pager\uFFFDwith NUL");
});

test("a revoked grant (403 on progress) stops the goal; receipt is still sent", async (t) => {
  const fixture = await startJevFixture({ policy: nextPolicy });
  t.after(fixture.close);
  const jev = new JevClient({
    apiKey: FIXTURE_KEY,
    endpoint: fixture.endpoint,
  });
  const { calls, browser } = await run(t, {
    jev,
    progress: () => ({ statusCode: 403, body: { error: "not_authorized" } }),
  });
  assert.equal(browser.acts.length, 0);
  assert.equal(calls.receipts[0].status, "failed");
  assert.equal(calls.receipts[0].result.code, "grant_revoked");
});

test("a host without Jev declares it and fails goals without touching the tab", async (t) => {
  const { calls, browser } = await run(t, {
    jev: undefined,
    progress: () => ({ body: { status: "running" } }),
  });
  assert.deepEqual(calls.polls[0], { jev: false });
  assert.equal(browser.acts.length, 0);
  assert.equal(calls.receipts[0].result.code, "jev_not_configured");
  assert.equal(calls.receipts[0].result.dispatched, false);
});
