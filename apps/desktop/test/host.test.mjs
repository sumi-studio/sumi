import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { BrowserHostAgent } from "../dist/browser/host.js";

const tab = { runtimeId: "runtime", profileId: "profile", tabId: "tab" };
const credential = {
  attachment: { attachment_id: "attachment", tab, allow_actions: true },
  host_token: "host-secret-never-to-page",
};
function job() {
  return {
    job_id: "job",
    claim_expires_at: new Date(Date.now() + 30000).toISOString(),
    request: {
      attachment_id: "attachment",
      tab,
      method: "act",
      binding: {
        revision: 1,
        observationId: "observation",
        url: "https://example.com/",
      },
      action: { kind: "click", target: "t0" },
    },
  };
}
async function fixture(t, handle, browser) {
  const server = createServer(handle);
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(() => {
    server.closeAllConnections();
    server.close();
  });
  return new BrowserHostAgent({
    apiOrigin: `http://127.0.0.1:${server.address().port}`,
    credential,
    browser,
    tab,
  });
}
async function body(req) {
  let s = "";
  for await (const b of req) s += b;
  return JSON.parse(s);
}
test("lost completion response retries only the identical receipt, never the click", async (t) => {
  let polls = 0,
    completions = 0,
    actions = 0;
  const receipts = [];
  const bridge = await fixture(
    t,
    async (req, res) => {
      assert.equal(
        req.headers.authorization,
        `Bearer ${credential.host_token}`,
      );
      if (req.url.endsWith("/poll")) {
        polls++;
        res.end(JSON.stringify({ job: job() }));
      } else {
        receipts.push(await body(req));
        completions++;
        if (completions === 1) req.socket.destroy();
        else res.end("{}");
      }
    },
    {
      act: async (...args) => {
        actions++;
        assert.ok(!JSON.stringify(args).includes(credential.host_token));
        return { value: "literal \\u0000 and nul \0" };
      },
    },
  );
  await assert.rejects(() => bridge.tick());
  await bridge.tick();
  assert.equal(actions, 1);
  assert.equal(polls, 1);
  assert.equal(completions, 2);
  assert.deepEqual(receipts[0], receipts[1]);
  assert.equal(receipts[1].result.value.value, "literal \\u0000 and nul �");
});
test("lost claim response is never replayed; replacement tab request cannot dispatch", async (t) => {
  let polls = 0,
    actions = 0;
  let receipt;
  const bridge = await fixture(
    t,
    async (req, res) => {
      if (req.url.endsWith("/poll")) {
        polls++;
        if (polls === 1) {
          req.socket.destroy();
          return;
        }
        const value = job();
        value.request.tab = { ...tab, tabId: "replacement" };
        res.end(JSON.stringify({ job: value }));
      } else {
        receipt = await body(req);
        res.end("{}");
      }
    },
    {
      act: async () => {
        actions++;
      },
    },
  );
  await assert.rejects(() => bridge.tick());
  await bridge.tick();
  assert.equal(actions, 0);
  assert.equal(receipt.result.outcome, "not_dispatched");
});
test("revoked host stops polling and insecure remote endpoints are rejected", async (t) => {
  let polls = 0;
  const bridge = await fixture(
    t,
    (_req, res) => {
      polls++;
      res.writeHead(403);
      res.end("{}");
    },
    {},
  );
  await assert.rejects(() => bridge.tick());
  await bridge.tick();
  assert.equal(polls, 1);
  assert.throws(
    () =>
      new BrowserHostAgent({
        apiOrigin: "http://example.com",
        credential,
        browser: {},
        tab,
      }),
    /HTTPS/,
  );
});

test("a cancelled job's expired-claim conflict does not revoke the tab or block later jobs", async (t) => {
  let polls = 0;
  let completions = 0;
  let actions = 0;
  const receipts = [];
  const bridge = await fixture(
    t,
    async (req, res) => {
      if (req.url.endsWith("/poll")) {
        const command = job();
        command.job_id =
          ++polls === 1 ? "cancelled-after-dispatch" : "later-unrelated-job";
        res.end(JSON.stringify({ job: command }));
      } else {
        receipts.push(await body(req));
        completions++;
        if (completions === 1) {
          // The command was cancelled after dispatch; completion transmission
          // failed, and the claim expired to lost before the retry arrived.
          req.socket.destroy();
        } else if (completions === 2) {
          res.writeHead(409);
          res.end('{"error":"result_conflict"}');
        } else res.end("{}");
      }
    },
    {
      act: async () => {
        actions++;
        return { status: "dispatched" };
      },
    },
  );
  await assert.rejects(() => bridge.tick());
  await assert.rejects(() => bridge.tick());
  assert.deepEqual(
    receipts[0],
    receipts[1],
    "retry only the old result, not its action",
  );
  await bridge.tick();
  assert.equal(polls, 2, "a command conflict is not attachment revocation");
  assert.equal(actions, 2, "one old action and one distinct later action");
  assert.equal(receipts[2].job_id, "later-unrelated-job");
});
