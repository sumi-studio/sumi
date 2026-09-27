import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { Secretary } from "../src/secretary.ts";
import { HttpStateClient } from "../src/state-client.ts";

let seq = 0,
  started = false,
  finished = false,
  pending = "",
  attachment = "",
  stage = 0;
const processed = new Set();
const provider = {
  name: "browser-acceptance",
  async *stream(req) {
    let next;
    const latest = req.messages.findLast((m) => m.role === "tool");
    const key = latest ? `${req.turnId}:${latest.toolCallId}` : "";
    if (latest && !processed.has(key)) {
      processed.add(key);
      const value = JSON.parse(latest.content);
      console.log(JSON.stringify({ tool: latest.name, value }));
      if (latest.name === "browser.tabs") {
        const match = value.tabs.find(
          (t) => t.name === "Acceptance tab" && t.available,
        );
        assert.ok(match, "secretary discovers its available real tab");
        attachment = match.attachment_id;
        next = {
          name: "browser.observe",
          arguments: { attachment_id: attachment },
        };
      } else if (
        latest.name === "browser.observe" ||
        latest.name === "browser.act"
      )
        pending = value.job.job_id;
      else if (latest.name === "job.status") {
        const job = value.job;
        assert.ok(
          !["failed", "lost", "cancelled"].includes(job.status),
          JSON.stringify(job),
        );
        if (job.status === "done") {
          pending = "";
          const result = job.result.value;
          if (stage === 0) {
            assert.equal(
              result.targets.find((t) => t.name === "Message").value,
              "human through real input",
            );
            next = {
              name: "browser.act",
              arguments: {
                attachment_id: attachment,
                binding: result.binding,
                action: {
                  kind: "fill",
                  target: result.targets.find((t) => t.name === "Message").id,
                  text: "secretary through authorized Core",
                },
              },
            };
          }
          if (stage === 1 || stage === 3)
            next = {
              name: "browser.observe",
              arguments: { attachment_id: attachment },
            };
          if (stage === 2) {
            assert.equal(
              result.targets.find((t) => t.name === "Message").value,
              "secretary through authorized Core",
            );
            next = {
              name: "browser.act",
              arguments: {
                attachment_id: attachment,
                binding: result.binding,
                action: {
                  kind: "click",
                  target: result.targets.find((t) => t.name === "Save").id,
                },
              },
            };
          }
          if (stage === 4) {
            assert.ok(
              result.text.includes(
                "secretary through authorized Core / clicks 1",
              ),
            );
            finished = true;
          }
          stage++;
        }
      }
    }
    if (!started) {
      started = true;
      next = { name: "browser.tabs", arguments: {} };
    } else if (!next && pending && req.round === 0)
      next = { name: "job.status", arguments: { job_id: pending } };
    if (next) {
      assert.ok(req.tools.some((t) => t.name === next.name));
      yield {
        type: "tool_call",
        call: { id: `browser-${seq++}`, route: "normal", ...next },
      };
    } else
      yield {
        type: "text",
        delta: finished
          ? "Same document verified."
          : "Waiting for browser result.",
      };
    yield { type: "done", usage: {} };
  },
};
const secretary = new Secretary({
  personaId: process.env.BROWSER_TEST_PERSONA,
  holderId: "browser-acceptance",
  state: new HttpStateClient(
    process.env.BROWSER_TEST_URL,
    process.env.BROWSER_TEST_TOKEN,
  ),
  provider,
  leaseTtlMs: 30000,
  renewEveryMs: 5000,
  contextLimit: 100,
  pollIntervalMs: 50,
  scheduleEveryMs: 1000000,
  idgen: () => crypto.randomUUID(),
});
await secretary.start();
try {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline && !finished) {
    await secretary.step();
    await delay(30);
  }
  assert.ok(
    finished,
    "Secretary observed human input, acted, and observed same-tab result through durable jobs",
  );
  console.log("PASS secretary same-tab end-to-end");
} finally {
  await secretary.stop();
}
