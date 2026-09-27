// Scripted secretary for TestSecretaryJevGoalRealBrowser. A deterministic model
// provider drives the real Secretary through durable Core tools:
// browser.tabs → browser.goal (done) → browser.goal (cancel while running)
// → browser.goal (the person presses Stop in the host strip) → browser.goal
// (Jev auth failure) → browser.tabs (Jev withdrawn) → direct observe/act.
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { Secretary } from "../src/secretary.ts";
import { HttpStateClient } from "../src/state-client.ts";

const site = process.env.BROWSER_TEST_SITE;
let seq = 0;
let attachment = "";
let pending = "";
let stage = "tabs";
let cancelled = false;
let finished = false;
let pagerBefore = "";
let tabsRetries = 0;
const processed = new Set();
const log = (entry) => console.log(JSON.stringify(entry));

function onResult(name, value) {
  if (name === "browser.tabs" && stage === "tabs-after-outage") {
    const tab = value.tabs.find((t) => t.attachment_id === attachment);
    log({ tabs_after_outage: tab.operation_layers });
    // The host withdraws Jev after its key was rejected, on its next poll
    // (≤250 ms after the receipt); direct stays.
    if (tab.operation_layers.jev === "available" && ++tabsRetries < 20)
      return { name: "browser.tabs", arguments: {} };
    assert.deepEqual(tab.operation_layers, { direct: true, jev: "not_configured" });
    stage = "direct-observe";
    return { name: "browser.observe", arguments: { attachment_id: attachment } };
  }
  if (name === "browser.tabs") {
    const tab = value.tabs.find((t) => t.name === "Jev acceptance tab" && t.available);
    assert.ok(tab, "secretary discovers its tab");
    assert.deepEqual(tab.operation_layers, { direct: true, jev: "available" });
    attachment = tab.attachment_id;
    stage = "signup";
    return {
      name: "browser.goal",
      arguments: {
        attachment_id: attachment,
        goal: "Sign up for the newsletter with my name and email, then save.",
        inputs: { name: "Ada Lovelace", email: "ada@example.test" },
        max_steps: 8,
      },
    };
  }
  if (["browser.goal", "browser.observe", "browser.act"].includes(name)) {
    pending = value.job.job_id;
    return undefined;
  }
  if (name === "job.cancel") {
    cancelled = true;
    return undefined;
  }
  if (name !== "job.status") return undefined;
  const job = value.job;
  if (!["done", "failed", "cancelled", "lost"].includes(job.status)) {
    // Observe progress of a running goal; stop the pager goal mid-run.
    if (
      stage === "pager" &&
      !cancelled &&
      (job.result?.progress?.actions_dispatched ?? 0) >= 2
    ) {
      log({ progress_seen: job.result.progress });
      return { name: "job.cancel", arguments: { job_id: pending } };
    }
    return undefined;
  }
  pending = "";
  const v = job.result?.value;
  if (stage === "signup") {
    assert.equal(job.status, "done", JSON.stringify(job));
    assert.equal(v.operation_layer, "jev");
    assert.equal(v.goal_outcome, "jev_reported_done");
    assert.ok(v.steps.some((s) => s.result === "refused:page_changed"));
    assert.ok(v.final_page.text.includes("Saved Ada Lovelace <ada@example.test> x1"));
    stage = "pager";
    return {
      name: "browser.goal",
      arguments: {
        attachment_id: attachment,
        goal: "Open the pager and press Next until it shows Page 30.",
        inputs: { start_url: `${site}/pager` },
        max_steps: 30,
      },
    };
  }
  if (stage === "pager") {
    assert.equal(job.status, "cancelled", JSON.stringify(job));
    assert.ok(job.cancel_requested_at);
    assert.equal(v.goal_outcome, "cancelled");
    assert.ok(v.actions_dispatched >= 2 && v.actions_dispatched < 30);
    assert.equal(v.steps[0].operation, "NAVIGATE");
    stage = "person-stop";
    return {
      name: "browser.goal",
      arguments: {
        attachment_id: attachment,
        goal: "Press Next until I stop you.",
        max_steps: 30,
      },
    };
  }
  if (stage === "person-stop") {
    assert.equal(job.status, "cancelled", JSON.stringify(job));
    assert.equal(job.cancel_requested_at ?? null, null, "not a secretary cancel");
    assert.equal(v.goal_outcome, "stopped_by_person");
    assert.equal(v.actions_dispatched, 2);
    log({ person_stop: { actions: v.actions_dispatched, reason: v.reason } });
    stage = "outage";
    return {
      name: "browser.goal",
      arguments: {
        attachment_id: attachment,
        goal: "Check the outage page.",
      },
    };
  }
  if (stage === "outage") {
    assert.equal(job.status, "failed");
    assert.equal(job.result.code, "jev_auth_failed");
    assert.equal(job.result.dispatched, false);
    assert.equal(v.jev.calls, 0);
    stage = "tabs-after-outage";
    return { name: "browser.tabs", arguments: {} };
  }
  if (stage === "direct-observe") {
    assert.equal(job.status, "done");
    pagerBefore = job.result.value.text;
    const next = job.result.value.targets.find((t) => t.name === "Next");
    stage = "direct-act";
    return {
      name: "browser.act",
      arguments: {
        attachment_id: attachment,
        binding: job.result.value.binding,
        action: { kind: "click", target: next.id },
      },
    };
  }
  if (stage === "direct-act") {
    assert.equal(job.status, "done");
    stage = "direct-verify";
    return { name: "browser.observe", arguments: { attachment_id: attachment } };
  }
  if (stage === "direct-verify") {
    const before = Number(/Page (\d+)/.exec(pagerBefore)[1]);
    assert.ok(job.result.value.text.includes(`Page ${before + 1}`));
    log({ direct_fallback: { before, after: before + 1 } });
    finished = true;
  }
  return undefined;
}

let started = false;
const provider = {
  name: "browser-goal-acceptance",
  async *stream(req) {
    let next;
    const latest = req.messages.findLast((m) => m.role === "tool");
    const key = latest ? `${req.turnId}:${latest.toolCallId}` : "";
    if (latest && !processed.has(key)) {
      processed.add(key);
      const value = JSON.parse(latest.content);
      log({ tool: latest.name, stage, status: value.job?.status });
      next = onResult(latest.name, value);
    }
    if (!started) {
      started = true;
      next = { name: "browser.tabs", arguments: {} };
    } else if (!next && pending && !finished) {
      await delay(250);
      next = { name: "job.status", arguments: { job_id: pending } };
    }
    if (next) {
      assert.ok(req.tools.some((t) => t.name === next.name), next.name);
      yield { type: "tool_call", call: { id: `goal-${seq++}`, route: "normal", ...next } };
    } else
      yield { type: "text", delta: finished ? "Verified." : "Waiting for the browser." };
    yield { type: "done", usage: {} };
  },
};
const secretary = new Secretary({
  personaId: process.env.BROWSER_TEST_PERSONA,
  holderId: "browser-goal-acceptance",
  state: new HttpStateClient(process.env.BROWSER_TEST_URL, process.env.BROWSER_TEST_TOKEN),
  provider,
  leaseTtlMs: 30000,
  renewEveryMs: 5000,
  contextLimit: 200,
  maxToolRounds: 200,
  pollIntervalMs: 50,
  scheduleEveryMs: 1000000,
  idgen: () => crypto.randomUUID(),
});
await secretary.start();
try {
  const deadline = Date.now() + 75000;
  while (Date.now() < deadline && !finished) {
    await secretary.step();
    await delay(30);
  }
  assert.ok(finished, `secretary finished all stages (stuck at ${stage})`);
  console.log("PASS secretary Jev goal end-to-end");
} finally {
  await secretary.stop();
}
