/**
 * Local job-runner host: a Node process that owns background job execution
 * for one persona, independent of the secretary process.
 *
 *   SUMI_STATE_URL=http://127.0.0.1:PORT  (required)
 *   SUMI_PERSONA_ID, SUMI_PERSONA_TOKEN   (required — persona-scoped token)
 *   SUMI_RUNNER_ID                        (default: runner-<pid>)
 *   SUMI_WORKSPACE_ROOT                   (required — job cwd confinement root)
 *   SUMI_JOB_LEASE_MS                     (default: 30000)
 *   SUMI_JOB_POLL_MS                      (default: 1000)
 *   SUMI_JOB_CONCURRENCY                  (default: 4)
 *   SUMI_JOB_MAX_OUTPUT_BYTES             (default: 262144)
 *   --once  run claim passes until no jobs are active/queued, then exit
 *           (used by the e2e harness)
 *
 * Kill -9 safe at any point: jobs it held are not re-run — their claims
 * expire and the next claim pass (any runner) sweeps them to 'lost'.
 */

import { JobRunner } from "../jobs/runner.ts";
import { HttpStateClient } from "../state-client.ts";

function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`missing env ${name}`);
  return v;
}

async function main() {
  const once = process.argv.includes("--once");
  const state = new HttpStateClient(
    env("SUMI_STATE_URL"),
    env("SUMI_PERSONA_TOKEN"),
  );
  const runner = new JobRunner({
    personaId: env("SUMI_PERSONA_ID"),
    runnerId: process.env.SUMI_RUNNER_ID ?? `runner-${process.pid}`,
    state,
    workspaceRoot: env("SUMI_WORKSPACE_ROOT"),
    leaseMs: Number(process.env.SUMI_JOB_LEASE_MS ?? 30_000),
    pollMs: Number(process.env.SUMI_JOB_POLL_MS ?? 1_000),
    concurrency: Number(process.env.SUMI_JOB_CONCURRENCY ?? 4),
    maxOutputBytes: Number(process.env.SUMI_JOB_MAX_OUTPUT_BYTES ?? 262_144),
    log: (msg, fields) =>
      console.log(`[runner] ${msg}`, fields ? JSON.stringify(fields) : ""),
  });

  if (once) {
    // --once: claim passes until no running jobs remain and a quiet window
    // has passed — bounded so a stuck job cannot hang CI forever.
    const deadline = Date.now() + 120_000;
    const idleGraceMs = Number(process.env.SUMI_ONCE_IDLE_MS ?? 3_000);
    let lastActivity = Date.now();
    while (Date.now() < deadline && Date.now() - lastActivity < idleGraceMs) {
      const before = runner.activeCount;
      await runner.step();
      if (runner.activeCount > 0 || runner.activeCount !== before) {
        lastActivity = Date.now();
      }
      await new Promise((res) => setTimeout(res, 200));
    }
    await runner.stop();
    return;
  }

  const ac = new AbortController();
  process.on("SIGINT", () => ac.abort());
  process.on("SIGTERM", () => ac.abort());
  await runner.run(ac.signal);
}

main().catch((e) => {
  console.error("[runner] fatal:", e);
  process.exit(1);
});
