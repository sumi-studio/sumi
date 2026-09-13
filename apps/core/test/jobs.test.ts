import assert from "node:assert/strict";
import { test } from "node:test";
import { FakeState } from "../src/fake-state.ts";
import { JobRunner } from "../src/jobs/runner.ts";
import type {
  ModelEvent,
  ModelProvider,
  ModelRequest,
} from "../src/provider.ts";
import { Secretary, type SecretaryConfig } from "../src/secretary.ts";
import { type StateClient, StateError } from "../src/state-client.ts";

const PERSONA = "01930e00-0000-7000-8000-0000000000aa";

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function until(
  cond: () => boolean,
  ms = 5_000,
  step = 25,
): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (cond()) return;
    await sleep(step);
  }
  assert.ok(cond(), "condition not met within timeout");
}

function newState(): FakeState {
  const s = new FakeState();
  s.addPersona(PERSONA, "Test");
  return s;
}

class ScriptedProvider implements ModelProvider {
  readonly name = "scripted";
  consultations = 0;
  private readonly script: {
    text: string;
    calls?: { tool: string; request: Record<string, unknown> }[];
  };
  constructor(script: {
    text: string;
    calls?: { tool: string; request: Record<string, unknown> }[];
  }) {
    this.script = script;
  }
  async *stream(_req: ModelRequest): AsyncIterable<ModelEvent> {
    this.consultations++;
    yield { type: "text", delta: this.script.text };
    for (const [i, c] of (this.script.calls ?? []).entries()) {
      yield {
        type: "tool_call",
        call: { id: `call-${i}`, name: c.tool, arguments: c.request },
      };
    }
    yield { type: "done", usage: {} };
  }
}

function secretaryCfg(
  state: StateClient,
  provider: ModelProvider,
): SecretaryConfig {
  return {
    personaId: PERSONA,
    holderId: "test-sec",
    state,
    provider,
    leaseTtlMs: 30_000,
    renewEveryMs: 1_000,
    contextLimit: 20,
    pollIntervalMs: 1,
    scheduleEveryMs: 1_000_000,
    idgen: () => crypto.randomUUID(),
  };
}

// --- FakeState lifecycle --------------------------------------------------

test("job lifecycle: submit → claim → heartbeat → complete → one notification", async () => {
  const s = newState();
  const { job, created } = await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["echo", "hi"] },
  });
  assert.equal(created, true);
  assert.equal(job.status, "queued");

  // Identical resend replays; divergent resend conflicts.
  const again = await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["echo", "hi"] },
  });
  assert.equal(again.created, false);
  await assert.rejects(
    s.submitJob(PERSONA, {
      jobId: "j-1",
      kind: "subprocess",
      request: { command: ["echo", "other"] },
    }),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );

  const { claimed, swept } = await s.claimJobs(PERSONA, {
    runnerId: "r-1",
    kinds: ["subprocess"],
    leaseMs: 30_000,
  });
  assert.equal(swept.length, 0);
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0]?.status, "running");
  assert.equal(claimed[0]?.claimed_by, "r-1");

  const hb = await s.heartbeatJob(PERSONA, "j-1", {
    runnerId: "r-1",
    leaseMs: 30_000,
  });
  assert.equal(hb.status, "running");
  await assert.rejects(
    s.heartbeatJob(PERSONA, "j-1", { runnerId: "r-2", leaseMs: 30_000 }),
    (e: unknown) => e instanceof StateError && e.status === 409,
  );

  const result = {
    exit_code: 0,
    stdout: "hi\n",
    stderr: "",
    stdout_truncated: false,
    stderr_truncated: false,
  };
  const done = await s.completeJob(PERSONA, "j-1", {
    runnerId: "r-1",
    status: "done",
    result,
  });
  assert.equal(done.status, "done");
  assert.ok(done.notified_at);

  // Exactly one notification input, under the reserved job: prefix.
  const notes = s.inputs.filter((i) => i.input_id === "job:j-1");
  assert.equal(notes.length, 1);
  assert.equal(notes[0]?.kind, "job_completed");
  assert.equal(notes[0]?.status, "queued");

  // Identical replay returns the stored row — no second notification.
  const replay = await s.completeJob(PERSONA, "j-1", {
    runnerId: "r-1",
    status: "done",
    result,
  });
  assert.equal(replay.status, "done");
  assert.equal(s.inputs.filter((i) => i.input_id === "job:j-1").length, 1);
  // Divergent replay conflicts and carries the stored row.
  await assert.rejects(
    s.completeJob(PERSONA, "j-1", {
      runnerId: "r-1",
      status: "failed",
      result,
      error: "x",
    }),
    (e: unknown) =>
      e instanceof StateError && e.status === 409 && e.job?.status === "done",
  );
});

test("cancel: queued → cancelled+notify; running → cancel_requested; expired claim → lost", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-q",
    kind: "subprocess",
    request: { command: ["sleep", "9"] },
  });
  const cancelled = await s.cancelJob(PERSONA, "j-q");
  assert.equal(cancelled.status, "cancelled");
  assert.ok(s.inputs.some((i) => i.input_id === "job:j-q"));

  await s.submitJob(PERSONA, {
    jobId: "j-r",
    kind: "subprocess",
    request: { command: ["sleep", "9"] },
  });
  await s.claimJobs(PERSONA, {
    runnerId: "r-1",
    kinds: ["subprocess"],
    leaseMs: 30_000,
  });
  const cr = await s.cancelJob(PERSONA, "j-r");
  assert.equal(cr.status, "cancel_requested");
  const hb = await s.heartbeatJob(PERSONA, "j-r", {
    runnerId: "r-1",
    leaseMs: 30_000,
  });
  assert.equal(hb.status, "cancel_requested");
  const done = await s.completeJob(PERSONA, "j-r", {
    runnerId: "r-1",
    status: "cancelled",
    result: { signal: "SIGTERM" },
  });
  assert.equal(done.status, "cancelled");

  // Expired claim sweeps to lost on the next claim pass — never re-queued.
  await s.submitJob(PERSONA, {
    jobId: "j-l",
    kind: "subprocess",
    request: { command: ["sleep", "9"] },
  });
  await s.claimJobs(PERSONA, {
    runnerId: "r-1",
    kinds: ["subprocess"],
    leaseMs: 30,
  });
  await sleep(60);
  const pass = await s.claimJobs(PERSONA, {
    runnerId: "r-2",
    kinds: ["subprocess"],
    leaseMs: 30_000,
  });
  assert.equal(pass.swept.length, 1);
  assert.equal(pass.swept[0]?.status, "lost");
  const lost = await s.getJob(PERSONA, "j-l");
  assert.equal(lost.status, "lost");
  assert.ok(lost.error?.includes("indeterminate"));
  await assert.rejects(
    s.completeJob(PERSONA, "j-l", {
      runnerId: "r-1",
      status: "done",
      result: {},
    }),
    (e: unknown) =>
      e instanceof StateError && e.status === 409 && e.job?.status === "lost",
  );
  const pass2 = await s.claimJobs(PERSONA, {
    runnerId: "r-2",
    kinds: ["subprocess"],
    leaseMs: 30_000,
  });
  assert.equal(pass2.claimed.length, 0);
});

// --- Secretary path -------------------------------------------------------

// job.start mints a queued job inside the plan-bound claim; the job_id is
// server-derived so the model cannot choose an idempotency identity.
test("secretary job.start creates a queued job with a derived id", async () => {
  const s = newState();
  const provider = new ScriptedProvider({
    text: "running in background",
    calls: [
      {
        tool: "job.start",
        request: { command: ["echo", "hello"], timeout_ms: 5000 },
      },
    ],
  });
  const sec = new Secretary(secretaryCfg(s, provider));
  s.addInput(PERSONA, "in-1", "run echo please");
  await sec.start();
  await sec.step();

  const jobs = await s.listJobs(PERSONA);
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0]?.job_id, "op:in-1:0");
  assert.equal(jobs[0]?.status, "queued");
  assert.equal(jobs[0]?.kind, "subprocess");
  assert.equal(provider.consultations, 1);
});

// A job_completed notification is an ordinary input: the same continuing
// secretary claims it on its next turn — no side channel, no second agent.
test("job_completed notification is claimed by the same secretary", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["echo", "hi"] },
  });
  await s.claimJobs(PERSONA, {
    runnerId: "r-1",
    kinds: ["subprocess"],
    leaseMs: 30_000,
  });
  await s.completeJob(PERSONA, "j-1", {
    runnerId: "r-1",
    status: "done",
    result: { exit_code: 0, stdout: "hi\n" },
  });

  const provider = new ScriptedProvider({ text: "the echo finished" });
  const sec = new Secretary(secretaryCfg(s, provider));
  await sec.start();
  await sec.step();
  const note = s.inputs.find((i) => i.input_id === "job:j-1");
  assert.equal(note?.status, "done");
  assert.equal(provider.consultations, 1);
  // Consumed once: the next step is idle, not a second delivery.
  assert.equal(await sec.step(), "idle");
});

// --- Runner ---------------------------------------------------------------

function runnerCfg(
  state: StateClient,
  over: Partial<ConstructorParameters<typeof JobRunner>[0]> = {},
) {
  return {
    personaId: PERSONA,
    runnerId: "r-test",
    state,
    workspaceRoot: process.cwd(),
    leaseMs: 900,
    pollMs: 50,
    ...over,
  };
}

test("runner completes a real subprocess and records bounded output", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sh", "-c", "echo out; echo err >&2; exit 3"] },
  });
  const r = new JobRunner(runnerCfg(s));
  await r.step();
  await until(() => r.activeCount === 0);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "failed"); // exit 3 — honestly recorded
  assert.equal(job.result?.exit_code, 3);
  assert.equal(job.result?.stdout, "out\n");
  assert.equal(job.result?.stderr, "err\n");
  assert.ok(s.inputs.some((i) => i.input_id === "job:j-1"));
  await r.stop();
});

test("runner observes cancel via heartbeat and kills the subprocess", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sleep", "60"] },
  });
  const r = new JobRunner(runnerCfg(s));
  await r.step();
  await until(() => r.activeCount === 1);
  await s.cancelJob(PERSONA, "j-1");
  await until(() => r.activeCount === 0, 10_000);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "cancelled");
  assert.equal(job.result?.signal, "SIGTERM");
  await r.stop();
});

test("runner retries an identical completion after a lost response", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["echo", "done"] },
  });
  // Wrap the fake: the first complete call "loses" the response — the job
  // records durably, but the runner sees a failure and must retry the
  // identical body (which then replays the stored row).
  let dropped = false;
  const wrapped: StateClient = Object.create(s);
  const orig = s.completeJob.bind(s);
  wrapped.completeJob = async (persona, jobId, req) => {
    const job = await orig(persona, jobId, req);
    if (!dropped) {
      dropped = true;
      throw new StateError(500, "simulated lost response");
    }
    return job;
  };
  const r = new JobRunner(runnerCfg(wrapped));
  await r.step();
  await until(() => r.activeCount === 0);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "done");
  assert.equal(job.result?.stdout, "done\n");
  assert.equal(s.inputs.filter((i) => i.input_id === "job:j-1").length, 1);
  await r.stop();
});

test("a timeout marks the job failed without waiting forever", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sleep", "60"], timeout_ms: 150 },
  });
  const r = new JobRunner(runnerCfg(s));
  await r.step();
  await until(() => r.activeCount === 0, 10_000);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "failed");
  assert.ok(job.error?.includes("timeout"));
  assert.equal(job.result?.timed_out, true);
  await r.stop();
});

test("expired claim sweeps to lost; the late runner cannot overwrite it", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sleep", "60"] },
  });
  // Short lease; every heartbeat fails so the claim expires mid-run.
  let down = true;
  const wrapped: StateClient = Object.create(s);
  wrapped.heartbeatJob = async (persona, jobId, req) => {
    if (down) throw new StateError(500, "simulated runner network loss");
    return s.heartbeatJob(persona, jobId, req);
  };
  const r = new JobRunner(runnerCfg(wrapped, { leaseMs: 200 }));
  await r.step();
  await until(() => r.activeCount === 1);
  await sleep(400); // claim expires while heartbeats fail
  // Another claim pass sweeps the expired job to 'lost'.
  const { swept } = await s.claimJobs(PERSONA, {
    runnerId: "r-other",
    kinds: ["subprocess"],
    leaseMs: 30_000,
  });
  assert.equal(swept.length, 1);
  assert.equal(swept[0]?.status, "lost");
  // Restore heartbeats: the runner learns its ownership is gone (409 with
  // the stored row) and kills the child — the 'lost' verdict stands.
  down = false;
  await until(() => r.activeCount === 0, 10_000);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "lost");
  assert.ok(s.inputs.some((i) => i.input_id === "job:j-1"));
  await r.stop();
});

test("graceful runner stop records the kill it performed", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sleep", "60"] },
  });
  const r = new JobRunner(runnerCfg(s));
  await r.step();
  await until(() => r.activeCount === 1);
  await r.stop();
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "failed");
  assert.ok(job.error?.includes("runner stopped"));
});

// --- Result accuracy (review D1/D2) ---------------------------------------

// D1: output containing NUL cannot be stored in jsonb. The runner scrubs it
// to U+FFFD and flags the rendering — the known exit-0 outcome is recorded
// 'done', not stranded as 'lost'.
test("runner scrubs NUL output and records the known outcome", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sh", "-c", "printf 'a\\0b\\n'"] },
  });
  const r = new JobRunner(runnerCfg(s));
  await r.step();
  await until(() => r.activeCount === 0);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "done");
  assert.equal(job.result?.exit_code, 0);
  assert.equal(job.result?.stdout, "a\uFFFDb\n");
  assert.equal(job.result?.stdout_sanitized, true);
  assert.ok(s.inputs.some((i) => i.input_id === "job:j-1"));
  await r.stop();
});

// D1 fallback: a completion payload the service deterministically rejects
// (400) is degraded once to a minimal honest record with the same observed
// status — the job is not stranded as 'lost'.
test("a 400-rejected completion degrades to a minimal honest record", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["echo", "hi"] },
  });
  // The fake rejects the full result payload once (as the real service
  // would for an un-storable field), then accepts the degraded record.
  const wrapped: StateClient = Object.create(s);
  const orig = s.completeJob.bind(s);
  let rejected = false;
  wrapped.completeJob = async (persona, jobId, req) => {
    if (!rejected && Object.keys(req.result ?? {}).length > 0) {
      rejected = true;
      throw new StateError(400, "job result contains a NUL byte");
    }
    return orig(persona, jobId, req);
  };
  const r = new JobRunner(runnerCfg(wrapped));
  await r.step();
  await until(() => r.activeCount === 0);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "done"); // observed status preserved
  assert.equal(job.result?.payload_rejected, true);
  assert.ok(job.error?.includes("rejected deterministically"));
  assert.ok(s.inputs.some((i) => i.input_id === "job:j-1"));
  await r.stop();
});

// D2: an external signal with no cancel request is a failure, not a
// cancellation — 'cancelled' is reserved for the observed cancel flow.
test("an external signal without a cancel request records failed", async () => {
  const s = newState();
  await s.submitJob(PERSONA, {
    jobId: "j-1",
    kind: "subprocess",
    request: { command: ["sh", "-c", "kill -SEGV $$"] },
  });
  const r = new JobRunner(runnerCfg(s));
  await r.step();
  await until(() => r.activeCount === 0);
  const job = await s.getJob(PERSONA, "j-1");
  assert.equal(job.status, "failed");
  assert.equal(job.result?.signal, "SIGSEGV");
  assert.ok(job.error?.includes("SIGSEGV"));
  await r.stop();
});
