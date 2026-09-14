#!/usr/bin/env node
/**
 * E2E for secretary-independent jobs (M09): REAL Go state service + REAL
 * PostgreSQL + a real Node job-runner process + real secretary attempts.
 *
 * Covers the user outcome directly: a background command continues while
 * the secretary process is down; its result is persisted and delivered to
 * the same secretary when it resumes — without re-running the completed
 * effect and without consuming a duplicate notification.
 *
 *   1. Direct submit → runner runs a real subprocess while no secretary
 *      exists → complete → resumed secretary claims the 'job:<id>'
 *      notification through its ordinary input path, exactly once.
 *   2. job.start tool: the secretary's plan-bound claim mints the job row
 *      (server-derived id); a replayed claim cannot mint a second job.
 *   3. Lost completion response: identical complete replay returns the
 *      stored row; one notification total; divergent replay → 409.
 *   4. Cancel: queued → cancelled+notify; running → cancel_requested →
 *      runner observes via heartbeat → SIGTERM → 'cancelled'. A command
 *      that already exited records its real outcome ('done') with
 *      cancel_requested_at kept as evidence.
 *   5. Runner crash (kill -9): the claim expires, the next claim pass
 *      sweeps the job to 'lost' + notification; it is never re-executed.
 *   6. Completed effects are never re-run: marker files written by the
 *      subprocess prove one execution each.
 *   7. A job ends while its request is backing off: rounds 0-1 start two
 *      jobs, the process dies consulting round 2, the recovery's round 2 is
 *      rate-limited, and both jobs finish during the backoff. The job
 *      notifications are handled first and name the command and the
 *      unfinished request; the resumed request replays its receipts with
 *      each job's current state and executes nothing twice.
 *
 * Requires SUMI_TEST_DB_URL pointing at a database migrated to 0048.
 *   SUMI_TEST_DB_URL=postgres://sumi:sumi-dev@127.0.0.1:55432/sumi_core_jobs?sslmode=disable \
 *     node scripts/e2e-jobs.mjs
 *
 * Child mode (--child) runs one secretary attempt with a scripted provider.
 */
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const CHILD = process.argv.includes("--child");

if (CHILD) {
  await childMain();
} else {
  await main();
}

// ---------------------------------------------------------------- child ---
// One secretary attempt with a scripted provider — same shape as e2e-plan.
async function childMain() {
  const { Secretary } = await import("../src/secretary.ts");
  const { HttpStateClient } = await import("../src/state-client.ts");
  const { ModelError } = await import("../src/provider.ts");

  const env = (n) => {
    const v = process.env[n];
    if (!v) throw new Error(`missing env ${n}`);
    return v;
  };
  const script = JSON.parse(env("SUMI_SCRIPT"));

  class ScriptedProvider {
    name = "scripted";
    async *stream(req) {
      // Multi-round turns re-consult after committed tool results: the
      // script may be {rounds:[...]} (e2e-plan convention) or a flat
      // {text,calls} — the flat form is the round-0 decision; later rounds
      // default to text-only so the turn ends.
      const round = req.round ?? 0;
      console.log(`[child] MODEL CONSULTED round=${round}`);
      const decision = script.rounds
        ? (script.rounds[round] ?? { text: "", calls: [] })
        : round === 0
          ? script
          : { text: "", calls: [] };
      if (decision.crash) {
        console.log("[child] CRASH");
        process.exit(137);
      }
      if (decision.transient) {
        console.log("[child] TRANSIENT");
        throw new ModelError("scripted 429", {
          retryable: true,
          retryAfterMs: decision.retryAfterMs,
        });
      }
      if (script.logMessages) {
        // What this round was fed, for context assertions in the parent.
        for (const m of req.messages) {
          if (m.role === "user" || m.role === "tool") {
            console.log(
              `[child] FED round=${round} ${m.role} ${JSON.stringify(m.content)}`,
            );
          }
        }
      }
      yield { type: "text", delta: decision.text ?? "" };
      for (const [i, c] of (decision.calls ?? []).entries()) {
        yield {
          type: "tool_call",
          call: {
            id: `call-${round}-${i}`,
            name: c.tool,
            arguments: c.request,
          },
        };
      }
      yield { type: "done", usage: { scripted: true, round } };
    }
  }

  const state = new HttpStateClient(
    env("SUMI_STATE_URL"),
    env("SUMI_PERSONA_TOKEN"),
  );
  const secretary = new Secretary({
    personaId: env("SUMI_PERSONA_ID"),
    holderId: process.env.SUMI_HOLDER_ID ?? `jobs-e2e-${process.pid}`,
    state,
    provider: new ScriptedProvider(),
    leaseTtlMs: 30_000,
    renewEveryMs: 5_000,
    contextLimit: 60,
    pollIntervalMs: 100,
    scheduleEveryMs: 1_000_000,
    idgen: () => crypto.randomUUID(),
    log: (msg, fields) =>
      console.log(`[child] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });
  await secretary.start();
  const deadline = Date.now() + 30_000;
  let lastWork = Date.now();
  while (Date.now() < deadline && Date.now() - lastWork < 1_500) {
    const r = await secretary.step();
    if (r === "turn") lastWork = Date.now();
    else await new Promise((res) => setTimeout(res, 100));
  }
  await secretary.stop();
}

// --------------------------------------------------------------- parent ---
async function main() {
  const DB_URL = process.env.SUMI_TEST_DB_URL ?? process.env.SUMI_DB_URL;
  if (!DB_URL) {
    console.error("e2e-jobs: SUMI_TEST_DB_URL required — real PostgreSQL");
    process.exit(2);
  }
  const API_DIR = resolve(import.meta.dirname, "../../api");
  const SELF = resolve(import.meta.dirname, "e2e-jobs.mjs");
  const RUNNER_HOST = resolve(import.meta.dirname, "../src/host/job-runner.ts");
  // SUMI_E2E_PORT pins the port when parallel worktrees own port ranges.
  const PORT = Number(process.env.SUMI_E2E_PORT ?? 9450 + (process.pid % 10));
  const BASE = `http://127.0.0.1:${PORT}`;
  const ADMIN = `e2e-admin-${randomUUID().replaceAll("-", "")}`;
  const WORKSPACE = mkdtempSync(join(tmpdir(), "sumi-jobs-ws-"));

  const log = (...a) => console.log("[e2e-jobs]", ...a);
  const fail = (msg) => {
    console.error("[e2e-jobs] FAIL:", msg);
    process.exit(1);
  };
  const assert = (cond, msg, evidence) => {
    if (cond) return;
    if (evidence) console.error(evidence);
    fail(msg);
  };
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  async function until(fn, ms = 15_000, what = "condition") {
    const deadline = Date.now() + ms;
    for (;;) {
      const v = await fn();
      if (v) return v;
      if (Date.now() > deadline) fail(`timed out waiting for ${what}`);
      await sleep(150);
    }
  }

  function uuidv7() {
    const now = Date.now().toString(16).padStart(12, "0");
    const r = randomUUID().replaceAll("-", "");
    return `${now.slice(0, 8)}-${now.slice(8, 12)}-7${r.slice(13, 16)}-${((parseInt(r.slice(16, 18), 16) & 0x3f) | 0x80).toString(16).padStart(2, "0")}${r.slice(18, 20)}-${r.slice(20, 32)}`;
  }
  const personaId = uuidv7();

  async function req(method, path, token, body) {
    const res = await fetch(BASE + path, {
      method,
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await res.text();
    let json = null;
    try {
      json = JSON.parse(text);
    } catch {
      /* non-JSON */
    }
    return { status: res.status, json, text };
  }

  const markerLines = (name) => {
    const f = join(WORKSPACE, name);
    if (!existsSync(f)) return [];
    return readFileSync(f, "utf8").split("\n").filter(Boolean);
  };

  // --- boot the real stack -------------------------------------------------
  const binDir = mkdtempSync(join(tmpdir(), "sumi-e2e-jobs-"));
  const bin = join(binDir, "state-dev");
  log("building state-dev…");
  const build = spawnSync(
    "go",
    ["build", "-buildvcs=false", "-o", bin, "./cmd/state-dev"],
    { cwd: API_DIR, stdio: "inherit" },
  );
  if (build.status !== 0) fail("go build failed");

  log("starting state-dev on", BASE);
  const svc = spawn(bin, [], {
    env: {
      ...process.env,
      SUMI_DB_URL: DB_URL,
      SUMI_CORE_STATE_TOKEN: ADMIN,
      SUMI_STATE_LISTEN: `127.0.0.1:${PORT}`,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  svc.stderr.on("data", (d) => process.stderr.write(`[state-dev] ${d}`));
  process.on("exit", () => svc.kill("SIGKILL"));
  await until(
    async () => {
      try {
        return (await fetch(`${BASE}/health`)).ok;
      } catch {
        return false;
      }
    },
    15_000,
    "state-dev health",
  );

  const created = await req("POST", "/internal/core/personas", ADMIN, {
    persona_id: personaId,
    display_name: "e2e jobs secretary",
  });
  assert(created.status === 201, `createPersona ${created.status}`);
  const ptoken = created.json.persona_token;

  // --- helpers -------------------------------------------------------------
  const submitJob = (jobId, command, extra = {}) =>
    req("POST", `/internal/core/personas/${personaId}/jobs`, ptoken, {
      job_id: jobId,
      kind: "subprocess",
      request: { command, ...extra },
    });
  const getJob = (jobId) =>
    req(
      "GET",
      `/internal/core/personas/${personaId}/jobs/${encodeURIComponent(jobId)}`,
      ptoken,
    );
  const getInput = (inputId) =>
    req(
      "GET",
      `/internal/core/personas/${personaId}/inputs/${encodeURIComponent(inputId)}`,
      ptoken,
    );
  const completeJob = (jobId, runnerId, status, result, error = "") =>
    req(
      "POST",
      `/internal/core/personas/${personaId}/jobs/${encodeURIComponent(jobId)}/complete`,
      ptoken,
      {
        runner_id: runnerId,
        status,
        result,
        error,
      },
    );
  const claimJobs = (runnerId, leaseMs = 30_000) =>
    req("POST", `/internal/core/personas/${personaId}/jobs/claim`, ptoken, {
      runner_id: runnerId,
      kinds: ["subprocess"],
      lease_ms: leaseMs,
      limit: 8,
    });

  const runnerEnv = (extra = {}) => ({
    ...process.env,
    SUMI_STATE_URL: BASE,
    SUMI_PERSONA_ID: personaId,
    SUMI_PERSONA_TOKEN: ptoken,
    SUMI_WORKSPACE_ROOT: WORKSPACE,
    SUMI_JOB_POLL_MS: "200",
    ...extra,
  });
  const spawnRunner = (extra = {}) => {
    const p = spawn("node", [RUNNER_HOST], {
      env: runnerEnv(extra),
      stdio: ["ignore", "pipe", "pipe"],
    });
    p.stdout.on("data", (d) => process.stdout.write(`[runner] ${d}`));
    p.stderr.on("data", (d) => process.stderr.write(`[runner!] ${d}`));
    return p;
  };

  const secretaryEnv = (extra = {}) => ({
    ...process.env,
    SUMI_STATE_URL: BASE,
    SUMI_PERSONA_ID: personaId,
    SUMI_PERSONA_TOKEN: ptoken,
    SUMI_HOLDER_ID: "e2e-jobs-holder",
    ...extra,
  });
  const runSecretary = (extra, expectExit = 0) => {
    const r = spawnSync("node", [SELF, "--child"], {
      env: secretaryEnv(extra),
      encoding: "utf8",
      timeout: 90_000,
      maxBuffer: 64 * 1024 * 1024,
    });
    if (r.status !== expectExit) {
      console.error(r.stdout, r.stderr);
      fail(`secretary child exited ${r.status}, expected ${expectExit}`);
    }
    return r.stdout ?? "";
  };

  // --- scenario 1: the job outlives the secretary --------------------------
  // Submit a real command; run the runner to completion while NO secretary
  // process exists; then resume the secretary — the notification reaches it
  // through the ordinary input path, once.
  log(
    "scenario 1: job completes while secretary is down; resume delivers once",
  );
  {
    const r = await submitJob("j-s1", [
      "sh",
      "-c",
      "echo ran >> marker-s1.txt; sleep 0.3; echo hello-from-job",
    ]);
    assert(
      r.status === 201 && r.json.job.status === "queued",
      `submit: ${r.text}`,
    );

    const runner = spawnRunner({ SUMI_RUNNER_ID: "runner-s1" });
    await until(
      async () => (await getJob("j-s1")).json.job.status === "done",
      20_000,
      "j-s1 done",
    );
    runner.kill("SIGTERM");
    await until(
      () => runner.exitCode !== null || runner.signalCode !== null,
      10_000,
      "runner-s1 exit",
    );

    const job = (await getJob("j-s1")).json.job;
    assert(
      job.result.exit_code === 0,
      `exit_code: ${JSON.stringify(job.result)}`,
    );
    assert(
      job.result.stdout === "hello-from-job\n",
      `stdout: ${JSON.stringify(job.result)}`,
    );
    assert(
      markerLines("marker-s1.txt").length === 1,
      "effect ran exactly once",
    );

    // The notification input exists, queued, before the secretary wakes.
    const note = (await getInput("job:j-s1")).json.input;
    assert(
      note && note.status === "queued" && note.kind === "job_completed",
      `notification input: ${JSON.stringify(note)}`,
    );

    // Now the same secretary resumes and consumes it — exactly once.
    runSecretary({
      SUMI_SCRIPT: JSON.stringify({ text: "the job finished", calls: [] }),
    });
    const consumed = (await getInput("job:j-s1")).json.input;
    assert(
      consumed.status === "done",
      `notification consumed: ${consumed.status}`,
    );
    // A second secretary run finds nothing to re-deliver.
    const out = runSecretary({
      SUMI_SCRIPT: JSON.stringify({ text: "idle", calls: [] }),
    });
    assert(
      !out.includes("MODEL CONSULTED"),
      "notification re-delivered to a second turn",
      out,
    );
    assert(
      markerLines("marker-s1.txt").length === 1,
      "effect re-ran on resume",
    );
    log("  result persisted + delivered to the resumed secretary once");
  }

  // --- scenario 2: job.start via the secretary's plan ----------------------
  log("scenario 2: job.start tool mints the job inside the plan-bound claim");
  {
    const submit = await req(
      "POST",
      `/internal/core/personas/${personaId}/inputs`,
      ptoken,
      {
        input_id: "in-jobstart",
        kind: "message",
        payload: { text: "run the marker job in background" },
        actor_kind: "human",
        actor_id: "e2e",
        source_surface: "e2e",
      },
    );
    assert(submit.status === 201, `input submit: ${submit.text}`);

    runSecretary({
      SUMI_SCRIPT: JSON.stringify({
        text: "starting it in the background",
        calls: [
          {
            tool: "job.start",
            request: {
              command: ["sh", "-c", "echo ran >> marker-s2.txt; echo tool-out"],
            },
          },
        ],
      }),
    });

    const jobId = "op:in-jobstart:0";
    const job = (await getJob(jobId)).json.job;
    assert(
      job && job.status === "queued",
      `tool-minted job should be queued, got ${JSON.stringify(job)}`,
    );
    assert(job.created_by.startsWith("tool:"), `created_by: ${job.created_by}`);

    // Re-running the same turn path cannot mint a second job: the input is
    // done, so a replayed claim is impossible — and the durable row stands.
    const runner = spawnRunner({ SUMI_RUNNER_ID: "runner-s2" });
    await until(
      async () => (await getJob(jobId)).json.job.status === "done",
      20_000,
      "op job done",
    );
    runner.kill("SIGTERM");
    await until(
      () => runner.exitCode !== null || runner.signalCode !== null,
      10_000,
      "runner-s2 exit",
    );
    assert(
      markerLines("marker-s2.txt").length === 1,
      "tool job effect ran exactly once",
    );

    runSecretary({
      SUMI_SCRIPT: JSON.stringify({ text: "saw the result", calls: [] }),
    });
    const consumed = (await getInput(`job:${jobId}`)).json.input;
    assert(
      consumed.status === "done",
      `tool-job notification consumed: ${consumed.status}`,
    );
    log("  job.start → queued → executed once → notified once");
  }

  // --- scenario 3: lost completion response / replay ------------------------
  log(
    "scenario 3: identical complete replay → one row, one notification; divergent → 409",
  );
  {
    const r = await submitJob("j-s3", ["echo", "replay"]);
    assert(r.status === 201, `submit: ${r.text}`);
    const claim = await claimJobs("runner-s3");
    assert(claim.json.claimed.length === 1, `claim: ${claim.text}`);

    const result = {
      exit_code: 0,
      stdout: "replay\n",
      stderr: "",
      stdout_truncated: false,
      stderr_truncated: false,
    };
    const c1 = await completeJob("j-s3", "runner-s3", "done", result);
    assert(
      c1.status === 200 && c1.json.job.status === "done",
      `complete: ${c1.text}`,
    );
    // The response to the runner is lost; it retries the identical body.
    const c2 = await completeJob("j-s3", "runner-s3", "done", result);
    assert(
      c2.status === 200 && c2.json.job.status === "done",
      `replay: ${c2.text}`,
    );
    // Still exactly one notification.
    const note = (await getInput("job:j-s3")).json.input;
    assert(
      note && note.status === "queued",
      `one notification: ${JSON.stringify(note)}`,
    );
    // A divergent terminal report is rejected with the stored row.
    const c3 = await completeJob(
      "j-s3",
      "runner-s3",
      "failed",
      result,
      "different story",
    );
    assert(
      c3.status === 409 && c3.json.job.status === "done",
      `divergent: ${c3.text}`,
    );
    log("  replay idempotent; divergence rejected");
  }

  // --- scenario 4: cancellation --------------------------------------------
  log(
    "scenario 4: cancel — queued cancels at once; running is observed by the runner",
  );
  {
    // Queued cancel: never runs, notification queued.
    const r = await submitJob("j-s4a", [
      "sh",
      "-c",
      "echo ran >> marker-s4a.txt",
    ]);
    assert(r.status === 201, `submit: ${r.text}`);
    const c = await req(
      "POST",
      `/internal/core/personas/${personaId}/jobs/j-s4a/cancel`,
      ptoken,
      {},
    );
    assert(
      c.status === 200 && c.json.job.status === "cancelled",
      `queued cancel: ${c.text}`,
    );
    assert(
      !existsSync(join(WORKSPACE, "marker-s4a.txt")),
      "cancelled-queued job must not run",
    );
    const note = (await getInput("job:j-s4a")).json.input;
    assert(
      note && note.kind === "job_completed",
      `cancel notification: ${JSON.stringify(note)}`,
    );

    // Running cancel: the runner's heartbeat observes cancel_requested and
    // SIGTERMs the subprocess.
    await submitJob("j-s4b", ["sleep", "60"]);
    const runner = spawnRunner({ SUMI_RUNNER_ID: "runner-s4" });
    await until(
      async () => (await getJob("j-s4b")).json.job.status === "running",
      15_000,
      "j-s4b running",
    );
    const c2 = await req(
      "POST",
      `/internal/core/personas/${personaId}/jobs/j-s4b/cancel`,
      ptoken,
      {},
    );
    assert(
      c2.status === 200 && c2.json.job.status === "cancel_requested",
      `running cancel: ${c2.text}`,
    );
    await until(
      async () => {
        const s = (await getJob("j-s4b")).json.job.status;
        return s === "cancelled" || s === "done";
      },
      15_000,
      "j-s4b terminal",
    );
    const job = (await getJob("j-s4b")).json.job;
    assert(job.status === "cancelled", `expected cancelled, got ${job.status}`);
    assert(
      job.result.signal === "SIGTERM",
      `signal: ${JSON.stringify(job.result)}`,
    );
    assert(job.cancel_requested_at !== null, "cancel_requested_at recorded");

    // Late completion honesty: cancel lands while the command may already
    // have exited — the runner reports what it actually observed.
    await submitJob("j-s4c", ["sh", "-c", "echo fast"]);
    await until(
      async () =>
        ["running", "done"].includes((await getJob("j-s4c")).json.job.status),
      15_000,
      "j-s4c claimed",
    );
    await req(
      "POST",
      `/internal/core/personas/${personaId}/jobs/j-s4c/cancel`,
      ptoken,
      {},
    );
    await until(
      async () =>
        ["done", "cancelled"].includes((await getJob("j-s4c")).json.job.status),
      15_000,
      "j-s4c terminal",
    );
    const late = (await getJob("j-s4c")).json.job;
    assert(
      late.cancel_requested_at !== null || late.status === "done",
      `late job: ${JSON.stringify(late)}`,
    );
    assert(
      (await getInput("job:j-s4c")).json.input.status === "queued",
      "one notification for the late job",
    );

    runner.kill("SIGTERM");
    await until(
      () => runner.exitCode !== null || runner.signalCode !== null,
      10_000,
      "runner-s4 exit",
    );
    log(
      "  queued→cancelled; running→cancel_requested→SIGTERM→cancelled; late outcome honest",
    );
  }

  // --- scenario 5: runner crash → lost, never re-executed -------------------
  log("scenario 5: kill -9 the runner; expired claim sweeps to 'lost'");
  {
    // The command writes a marker 0.4s in; we kill the runner mid-flight so
    // the outcome is genuinely indeterminate (it may or may not have run).
    await submitJob("j-s5", [
      "sh",
      "-c",
      "sleep 0.4; echo ran >> marker-s5.txt; sleep 60",
    ]);
    const runner = spawnRunner({
      SUMI_RUNNER_ID: "runner-s5",
      SUMI_JOB_LEASE_MS: "800",
    });
    await until(
      async () => (await getJob("j-s5")).json.job.status === "running",
      15_000,
      "j-s5 running",
    );
    runner.kill("SIGKILL");
    await until(
      () => runner.exitCode !== null || runner.signalCode !== null,
      10_000,
      "runner-s5 -9",
    );

    // After the claim expires, another runner's claim pass sweeps it.
    await sleep(1_200); // let the 800ms lease lapse
    const sweep = await claimJobs("runner-s5b", 30_000);
    assert(
      sweep.json.swept.length === 1 && sweep.json.swept[0].job_id === "j-s5",
      `sweep: ${sweep.text}`,
    );
    const job = (await getJob("j-s5")).json.job;
    assert(job.status === "lost", `expected lost, got ${job.status}`);
    assert(/indeterminate/.test(job.error ?? ""), `error: ${job.error}`);
    const note = (await getInput("job:j-s5")).json.input;
    assert(
      note && note.status === "queued",
      `lost notification: ${JSON.stringify(note)}`,
    );

    // Never re-executed: a fresh claim pass finds nothing queued for it.
    const again = await claimJobs("runner-s5c", 30_000);
    assert(again.json.claimed.length === 0, `re-claim: ${again.text}`);
    assert(
      markerLines("marker-s5.txt").length <= 1,
      "lost job must not re-run",
    );

    // The dead runner's late complete is refused — the stored verdict wins.
    const c = await completeJob("j-s5", "runner-s5", "done", { exit_code: 0 });
    assert(c.status === 409, `dead-runner complete: ${c.text}`);

    // The resumed secretary learns of the loss through its input path.
    runSecretary({
      SUMI_SCRIPT: JSON.stringify({ text: "the job was lost", calls: [] }),
    });
    const consumed = (await getInput("job:j-s5")).json.input;
    assert(
      consumed.status === "done",
      `lost notification consumed: ${consumed.status}`,
    );
    log("  claim expiry → lost + notify; no re-execution; dead runner refused");
  }

  // --- scenario 6: result accuracy -----------------------------------------
  // NUL/binary output must not strand a known outcome as 'lost' (the runner
  // scrubs un-storable bytes and flags the rendering), and an external
  // signal with no cancel request is 'failed', not 'cancelled'.
  log(
    "scenario 6: NUL output records done+sanitized; external signal → failed",
  );
  {
    // printf emits a literal NUL byte — valid UTF-8 output that jsonb
    // cannot store. The runner must still durably record the exit-0 outcome.
    const r = await submitJob("j-s6a", ["sh", "-c", "printf 'a\\0b\\n'"]);
    assert(r.status === 201, `submit: ${r.text}`);
    const runner = spawnRunner({ SUMI_RUNNER_ID: "runner-s6" });
    await until(
      async () => (await getJob("j-s6a")).json.job.status === "done",
      20_000,
      "j-s6a done",
    );
    const nulJob = (await getJob("j-s6a")).json.job;
    assert(
      nulJob.result.stdout === "a\uFFFDb\n",
      `scrubbed stdout: ${JSON.stringify(nulJob.result.stdout)}`,
    );
    assert(
      nulJob.result.stdout_sanitized === true,
      `sanitized flag: ${JSON.stringify(nulJob.result)}`,
    );
    assert(
      nulJob.result.exit_code === 0,
      `exit_code: ${JSON.stringify(nulJob.result)}`,
    );
    const noteA = (await getInput("job:j-s6a")).json.input;
    assert(
      noteA && noteA.status === "queued" && noteA.kind === "job_completed",
      `notification: ${JSON.stringify(noteA)}`,
    );

    // An external SIGSEGV — no cancel was ever requested — is a failure.
    const r2 = await submitJob("j-s6b", ["sh", "-c", "kill -SEGV $$"]);
    assert(r2.status === 201, `submit: ${r2.text}`);
    await until(
      async () =>
        ["failed", "done", "cancelled"].includes(
          (await getJob("j-s6b")).json.job.status,
        ),
      20_000,
      "j-s6b terminal",
    );
    const segvJob = (await getJob("j-s6b")).json.job;
    assert(
      segvJob.status === "failed",
      `expected failed, got ${segvJob.status}`,
    );
    assert(
      segvJob.result.signal === "SIGSEGV",
      `signal: ${JSON.stringify(segvJob.result)}`,
    );
    assert(/SIGSEGV/.test(segvJob.error ?? ""), `error: ${segvJob.error}`);
    assert(
      segvJob.cancel_requested_at === null,
      "no cancel was requested — cancel_requested_at must stay null",
    );
    const noteB = (await getInput("job:j-s6b")).json.input;
    assert(noteB && noteB.status === "queued", "one notification");

    runner.kill("SIGTERM");
    await until(
      () => runner.exitCode !== null || runner.signalCode !== null,
      10_000,
      "runner-s6 exit",
    );
    log("  NUL output → done + sanitized flag; SIGSEGV → failed + signal");
  }

  // --- scenario 7: a job ends while its request is backing off -------------
  // Own persona: earlier scenarios leave queued notifications behind.
  log(
    "scenario 7: crash + rate-limited round; notification explains itself; resume sees current job state",
  );
  {
    const p7 = uuidv7();
    const made = await req("POST", "/internal/core/personas", ADMIN, {
      persona_id: p7,
      display_name: "e2e jobs secretary (backoff)",
    });
    assert(made.status === 201, `createPersona s7 ${made.status}`);
    const t7 = made.json.persona_token;
    const who = { SUMI_PERSONA_ID: p7, SUMI_PERSONA_TOKEN: t7 };
    const get7 = async (path) =>
      (await req("GET", `/internal/core/personas/${p7}${path}`, t7)).json;
    const job7 = async (id) =>
      (await get7(`/jobs/${encodeURIComponent(id)}`))?.job;
    const input7 = async (id) =>
      (await get7(`/inputs/${encodeURIComponent(id)}`))?.input;
    const consulted = (out) =>
      [...out.matchAll(/MODEL CONSULTED round=(\d+)/g)].map((m) =>
        Number(m[1]),
      );
    const fed = (out, round, role) =>
      [...out.matchAll(new RegExp(`FED round=${round} ${role} (.*)`, "g"))].map(
        (m) => JSON.parse(m[1]),
      );
    const script = (rounds) => ({
      ...who,
      SUMI_SCRIPT: JSON.stringify({ rounds, logMessages: true }),
    });

    const X = {
      command: ["sh", "-c", "echo ran >> marker-s7.txt; echo job-out"],
    };
    const R0 = {
      text: "starting",
      calls: [
        { tool: "journal.note", request: { text: "s7-note" } },
        { tool: "job.start", request: X },
      ],
    };
    const R1 = { text: "one more", calls: [{ tool: "job.start", request: X }] };
    const R2 = {
      text: "checking",
      calls: [{ tool: "job.status", request: { job_id: "op:in-s7:1" } }],
    };
    const R3 = { text: "both jobs finished", calls: [] };

    const sub = await req("POST", `/internal/core/personas/${p7}/inputs`, t7, {
      input_id: "in-s7",
      kind: "message",
      payload: { text: "run X twice in the background" },
      actor_kind: "human",
      actor_id: "e2e",
      source_surface: "e2e",
    });
    assert(sub.status === 201, `input submit: ${sub.text}`);

    // A: rounds 0 and 1 commit their effects; the process dies at round 2.
    const a = runSecretary(script([R0, R1, { crash: true }]), 137);
    assert(
      JSON.stringify(consulted(a)) === "[0,1,2]",
      `A consulted ${consulted(a)}`,
      a,
    );
    assert(
      (await job7("op:in-s7:1"))?.status === "queued" &&
        (await job7("op:in-s7:2"))?.status === "queued",
      "rounds 0 and 1 each started a job before the crash",
    );

    // B: recovery replays rounds 0-1; round 2 is rate-limited.
    const b = runSecretary(
      script([R0, R1, { transient: true, retryAfterMs: 8_000 }]),
    );
    assert(
      JSON.stringify(consulted(b)) === "[2]",
      `B consulted ${consulted(b)}`,
      b,
    );
    const deferred = await input7("in-s7");
    assert(
      deferred.status === "queued" &&
        Date.parse(deferred.not_before) > Date.now(),
      `in-s7 deferred: ${JSON.stringify(deferred)}`,
    );

    // Both jobs finish during the backoff.
    const runner = spawnRunner({ ...who, SUMI_RUNNER_ID: "runner-s7" });
    await until(
      async () =>
        (await job7("op:in-s7:1"))?.status === "done" &&
        (await job7("op:in-s7:2"))?.status === "done",
      20_000,
      "s7 jobs done",
    );
    runner.kill("SIGTERM");
    await until(
      () => runner.exitCode !== null || runner.signalCode !== null,
      10_000,
      "runner-s7 exit",
    );
    assert(markerLines("marker-s7.txt").length === 2, "each job ran once");

    // C: the notifications run before in-s7 and explain themselves.
    assert(
      Date.parse((await input7("in-s7")).not_before) > Date.now(),
      "in-s7 still backing off before the notification turns",
    );
    const c = runSecretary(script([{ text: "noted the job", calls: [] }]));
    const jobMsgs = fed(c, 0, "user").filter((m) => m.startsWith("[job]"));
    log("  fed to the notification turn:", JSON.stringify(jobMsgs.at(-1)));
    for (const id of ["op:in-s7:1", "op:in-s7:2"]) {
      const want = `[job] job ${id} (subprocess) done, exit 0 — command: sh -c echo ran >> marker-s7.txt; echo job-out — started by you for request in-s7: "run X twice in the background" (that request was not finished yet when this job ended)`;
      assert(jobMsgs.includes(want), `notification context for ${id}`, c);
      assert(
        (await input7(`job:${id}`))?.status === "done",
        `${id} notification handled`,
      );
    }
    assert(
      (await input7("in-s7")).status === "queued",
      "in-s7 still pending after the notifications",
    );

    // D: after the backoff the request resumes at round 2.
    await until(
      async () => {
        const i = await input7("in-s7");
        return !i.not_before || Date.parse(i.not_before) <= Date.now();
      },
      30_000,
      "in-s7 backoff elapsed",
    );
    const d = runSecretary(script([R0, R1, R2, R3]));
    assert(
      JSON.stringify(consulted(d)) === "[2,3]",
      `D consulted ${consulted(d)}`,
      d,
    );
    const starts = fed(d, 2, "tool")
      .map((t) => JSON.parse(t))
      .filter((t) => t.job);
    log(
      "  replayed receipts fed to round 2:",
      JSON.stringify(
        starts.map((t) => ({
          job_id: t.job.job_id,
          receipt_status: t.job.status,
          current_job: t.current_job,
        })),
      ),
    );
    assert(
      starts.length === 2 &&
        starts.every(
          (t) =>
            t.job.status === "queued" &&
            t.current_job?.status === "done" &&
            t.current_job?.exit_code === 0,
        ),
      `replayed job.start receipts carry current state: ${JSON.stringify(starts)}`,
    );
    const status = fed(d, 3, "tool")
      .map((t) => JSON.parse(t))
      .at(-1);
    assert(
      status?.job?.job_id === "op:in-s7:1" &&
        status.job.status === "done" &&
        status.current_job === undefined,
      `fresh job.status: ${JSON.stringify(status)}`,
    );
    assert((await input7("in-s7")).status === "done", "in-s7 done");
    assert(
      (await job7("op:in-s7:3")) === undefined,
      "job.status minted no job",
    );
    assert(markerLines("marker-s7.txt").length === 2, "no job re-ran");

    // E: nothing left to deliver.
    const e = runSecretary(script([{ text: "idle", calls: [] }]));
    assert(!e.includes("MODEL CONSULTED"), "nothing re-delivered", e);
    log(
      "  crash + backoff: jobs once each; notifications self-explanatory; resume sees current state",
    );
  }

  log("PASS");
  svc.kill("SIGKILL");
  process.exit(0);
}
